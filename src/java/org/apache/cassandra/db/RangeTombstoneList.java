/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.magicwerk.brownies.collections.GapList;
import java.util.Comparator;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data structure holding the range tombstones of a ColumnFamily.
 * <p>
 * This is essentially a sorted list of non-overlapping (tombstone) ranges.
 * <p>
 * A range tombstone has 4 elements: the start and end of the range covered,
 * and the deletion infos (markedAt timestamp and local deletion time). The
 * markedAt timestamp is what define the priority of 2 overlapping tombstones.
 * That is, given 2 tombstones [0, 10]@t1 and [5, 15]@t2, then if t2 > t1 (and
 * are the tombstones markedAt values), the 2nd tombstone take precedence over
 * the first one on [5, 10]. If such tombstones are added to a RangeTombstoneList,
 * the range tombstone list will store them as [[0, 5]@t1, [5, 15]@t2].
 * <p>
 * The only use of the local deletion time is to know when a given tombstone can
 * be purged, which will be done by the purge() method.
 */
public class RangeTombstoneList implements Iterable<RangeTombstone>
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTombstoneList.class);

    public static final Serializer serializer = new Serializer();

    private final Comparator<ByteBuffer> comparator;
    private final Comparator<Range> endsComparator;
    private final Comparator<Range> startsComparator;

    private GapList<Range> list;

    public RangeTombstoneList(Comparator<ByteBuffer> comparator, int capacity)
    {
        this.comparator = comparator;
        this.list = new GapList<Range>(capacity);
        this.endsComparator = new EndsComparator();
        this.startsComparator = new StartsComparator();
    }

    public RangeTombstoneList(RangeTombstoneList that)
    {
        this.comparator = that.comparator;
        this.list = that.list.copy();
        this.endsComparator = new EndsComparator();
        this.startsComparator = new StartsComparator();
    }


    public boolean isEmpty()
    {
        return list.isEmpty();
    }

    public int size()
    {
        return list.size();
    }

    public Comparator<ByteBuffer> comparator()
    {
        return comparator;
    }

    public RangeTombstoneList copy()
    {
        return new RangeTombstoneList(this);
    }

    public void add(RangeTombstone tombstone)
    {
        add(new Range(tombstone.min, tombstone.max, tombstone.data.markedForDeleteAt, tombstone.data.localDeletionTime));
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case),
     * but it doesn't assume it.
     */
    public void add(ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        add(new Range(start, end, markedAt, delTime));
    }

     /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case),
     * but it doesn't assume it.
     */
    public void add(Range range)
    {
        if (isEmpty())
        {
            list.add(0, range);
            return;
        }

        int c = comparator.compare(list.peekLast().end, range.start);

        // Fast path if we add in sorted order
        if (c < 0)
        {
            list.add(size(), range);
        }
        // Fast path if we add in sorted order #2
        else if (comparator.compare(range.end, list.peekFirst().start) < 0)
        {
            list.add(0, range);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            ByteBuffer end = range.end;
            range.end = range.start;
            int pos = Collections.binarySearch(list, range, endsComparator);
            range.end = end;
            insertFrom((pos >= 0 ? pos : -pos-1), range);
        }
    }

    /**
     * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
     */
    public void addAll(RangeTombstoneList tombstones)
    {
        if (tombstones.isEmpty())
            return;

        if (isEmpty())
        {
            copyArrays(tombstones, this);
            return;
        }

        /*
         * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
         * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, then
         * calling add() will be faster, otherwise it's merging that will be faster.
         *
         * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
         * tombstones, while the CF we're adding it to (the one in the memtable) has many. In that case, using add() is
         * likely going to be faster.
         *
         * In other cases however, like when diffing responses from multiple nodes, the tombstone lists we "merge" will
         * be likely sized, so using add() might be a bit inefficient.
         *
         * Roughly speaking (this ignore the fact that updating an element is not exactly constant but that's not a big
         * deal), if n is the size of this list and m is tombstones size, merging is O(n+m) while using add() is O(m*log(n)).
         *
         * But let's not crank up a logarithm computation for that. Long story short, merging will be a bad choice only
         * if this list size is lot bigger that the other one, so let's keep it simple.
         */
        if (size() > 10 * tombstones.size())
        {
            for (int i = 0; i < tombstones.size(); i++)
                add(tombstones.list.get(i));
        }
        else
        {
            int i = 0;
            int j = 0;
            while (i < size() && j < tombstones.size())
            {
                Range tomb = tombstones.list.get(j);
                Range local = list.get(i);
                if (comparator.compare(tomb.start, local.end) <= 0)
                {
                    insertFrom(i, tomb);
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Addds the remaining ones from tombstones if any (note that list.add will increment size if relevant).
            for (; j < tombstones.size(); j++)
                list.add(size(), tombstones.list.get(j));
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(ByteBuffer name, long timestamp)
    {
        int idx = searchInternal(name);
        return idx >= 0 && list.get(idx).markedAt >= timestamp;
    }

    /**
     * Returns a new {@link InOrderTester}.
     */
    InOrderTester inOrderTester()
    {
        return new InOrderTester();
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime search(ByteBuffer name) {
        int idx = searchInternal(name);
        if (idx >= 0)
        {
            Range range = list.get(idx);
            return new DeletionTime(range.markedAt, range.delTime);
        }
        return null;
    }

    private int searchInternal(ByteBuffer name)
    {
        if (isEmpty())
            return -1;

        Range search = new Range(name, name, 0, 0);

        int pos = Collections.binarySearch(list, search, startsComparator);
        if (pos >= 0)
        {
            // We're exactly on an interval start. The one subtility is that we need to check if
            // the previous is not equal to us and doesn't have a higher marked at
            if (pos > 0)
            {
                Range last = list.get(pos - 1);
                if (comparator.compare(name, last.end) == 0 && last.markedAt > list.get(pos).markedAt)
                    return pos-1;
            }
            return pos;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            int idx = -pos-2;
            if (idx < 0)
                return -1;

            return comparator.compare(name, list.get(idx).end) <= 0 ? idx : -1;
        }
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.NATIVE.sizeof(size());
        for (Range range : list)
        {
            dataSize += range.start.remaining() + range.end.remaining();
            dataSize += TypeSizes.NATIVE.sizeof(range.markedAt);
            dataSize += TypeSizes.NATIVE.sizeof(range.delTime);
        }
        return dataSize;
    }

    public long minMarkedAt()
    {
        long min = Long.MAX_VALUE;
        for (Range range : list)
            min = Math.min(min, range.markedAt);
        return min;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        for (Range range : list)
            max = Math.max(max, range.markedAt);
        return max;
    }

    public void updateAllTimestamp(long timestamp)
    {
        for (Range range : list)
            range.markedAt = timestamp;
    }

    /**
     * Removes all range tombstones whose local deletion time is older than gcBefore.
     */
    public void purge(int gcBefore)
    {
        for (int i = 0; i < size(); i++)
            if (list.get(i).delTime < gcBefore)
                list.remove(i--);
    }

    /**
     * Returns whether {@code purge(gcBefore)} would remove something or not.
     */
    public boolean hasPurgeableTombstones(int gcBefore)
    {
        for (Range range : list)
        {
            if (range.delTime < gcBefore)
                return true;
        }
        return false;
    }

    public Iterator<RangeTombstone> iterator()
    {
        return new AbstractIterator<RangeTombstone>()
        {
            private Iterator<Range> iter = list.iterator();

            protected RangeTombstone computeNext()
            {
                if (!iter.hasNext())
                    return endOfData();

                Range range = iter.next();
                RangeTombstone t = new RangeTombstone(range.start,  range.end, range.markedAt, range.delTime);
                return t;
            }
        };
    }

    /**
     * Evaluates a diff between superset (known to be all merged tombstones) and this list for read repair
     *
     * @return null if there is no difference
     */
    public RangeTombstoneList diff(RangeTombstoneList superset)
    {
        if (isEmpty())
            return superset;

        RangeTombstoneList diff = null;

        int j = 0; // index to iterate through our own list
        for (int i = 0; i < superset.size; i++)
        {
            // we can assume that this list is a subset of the superset list
            while (j < size && comparator.compare(starts[j], superset.starts[i]) < 0)
                j++;

            if (j >= size)
            {
                // we're at the end of our own list, add the remainder of the superset to the diff
                if (i < superset.size)
                {
                    if (diff == null)
                        diff = new RangeTombstoneList(comparator, superset.size - i);

                    for(int k = i; k < superset.size; k++)
                        diff.add(superset.starts[k], superset.ends[k], superset.markedAts[k], superset.delTimes[k]);
                }
                return diff;
            }

            // we don't care about local deletion time here, because it doesn't matter for read repair
            if (!starts[j].equals(superset.starts[i])
                || !ends[j].equals(superset.ends[i])
                || markedAts[j] != superset.markedAts[i])
            {
                if (diff == null)
                    diff = new RangeTombstoneList(comparator, Math.min(8, superset.size - i));
                diff.add(superset.starts[i], superset.ends[i], superset.markedAts[i], superset.delTimes[i]);
            }
        }

        return diff;
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof RangeTombstoneList))
            return false;
        RangeTombstoneList that = (RangeTombstoneList)o;
        if (size() != that.size())
            return false;

        for (int i = 0; i < size(); i++)
        {
            if (!list.get(i).equals(that.list.get(i)))
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = size();
        for (Range range : list)
        {
            result += range.start.hashCode() + range.end.hashCode();
            result += (int)(range.markedAt ^ (range.markedAt >>> 32));
            result += range.delTime;
        }
        return result;
    }

    private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst)
    {
        dst.list = src.list.copy();
    }

    /*
     * Inserts a new element starting at index i. This method assumes that:
     *    ends[i-1] <= start <= ends[i]
     *
     * A RangeTombstoneList is a list of range [s_0, e_0]...[s_n, e_n] such that:
     *   - s_i <= e_i
     *   - e_i <= s_i+1
     *   - if s_i == e_i and e_i == s_i+1 then s_i+1 < e_i+1
     * Basically, range are non overlapping except for their bound and in order. And while
     * we allow ranges with the same value for the start and end, we don't allow repeating
     * such range (so we can't have [0, 0][0, 0] even though it would respect the first 2
     * conditions).
     *
     */
    private void insertFrom(int i, Range incoming)
    {
        while (i < size())
        {
            assert i == 0 || comparator.compare(list.get(i-1).end, incoming.start) <= 0;

            Range current = list.get(i);
            int c = comparator.compare(incoming.start, current.end);
            assert c <= 0;
            if (c == 0)
            {
                // If start == ends[i], then we can insert from the next one (basically the new element
                // really start at the next element), except for the case where starts[i] == ends[i].
                // In this latter case, if we were to move to next element, we could end up with ...[x, x][x, x]...
                if (comparator.compare(current.start, current.end) == 0)
                {
                    // The current element cover a single value which is equal to the start of the inserted
                    // element. If the inserted element overwrites the current one, just remove the current
                    // (it's included in what we insert) and proceed with the insert.
                    if (incoming.markedAt > current.markedAt)
                    {
                        list.remove(i);
                        continue;
                    }

                    // Otherwise (the current singleton interval override the new one), we want to leave the
                    // current element and move to the next, unless start == end since that means the new element
                    // is in fact fully covered by the current one (so we're done)
                    if (comparator.compare(incoming.start, incoming.end) == 0)
                        return;
                }
                i++;
                continue;
            }

            // Do we overwrite the current element?
            if (incoming.markedAt > current.markedAt)
            {
                // We do overwrite.

                // First deal with what might come before the newly added one.
                if (comparator.compare(current.start, incoming.start) < 0)
                {
                    list.add(i, new Range(current.start, incoming.start, current.markedAt, current.delTime));
                    i++;
                    // We don't need to do the following line, but in spirit that's what we want to do
                    // list.set(i, start, ends[i], markedAts, delTime])
                }

                // now, start <= starts[i]

                // Does the new element stops before/at the current one,
                int endCmp = comparator.compare(incoming.end, current.start);
                if (endCmp <= 0)
                {
                    // Here start <= starts[i] and end <= starts[i]
                    // This means the current element is before the current one. However, one special
                    // case is if end == starts[i] and starts[i] == ends[i]. In that case,
                    // the new element entirely overwrite the current one and we can just overwrite
                    if (endCmp == 0 && comparator.compare(current.start, current.end) == 0)
                        list.set(i, incoming);
                    else
                        list.add(i, incoming);
                    return;
                }

                // Do we overwrite the current element fully?
                int cmp = comparator.compare(current.end, incoming.end);
                if (cmp <= 0)
                {
                    // We do overwrite fully:
                    // update the current element until it's end and continue
                    // on with the next element (with the new inserted start == current end).

                    // If we're on the last element, we can optimize
                    if (i == size()-1)
                    {
                        list.set(i, incoming);
                        return;
                    }

                    list.set(i, new Range(incoming.start, current.end, incoming.markedAt, incoming.delTime));
                    if (cmp == 0)
                        return;

                    incoming = new Range(current.end, incoming.end, incoming.markedAt, incoming.delTime);
                    i++;
                }
                else
                {
                    // We don't ovewrite fully. Insert the new interval, and then update the now next
                    // one to reflect the not overwritten parts. We're then done.
                    list.add(i, incoming);
                    i++;
                    list.set(i, new Range(incoming.end, current.end, current.markedAt, current.delTime));
                    return;
                }
            }
            else
            {
                // we don't overwrite the current element

                // If the new interval starts before the current one, insert that new interval
                if (comparator.compare(incoming.start, current.start) < 0)
                {
                    // If we stop before the start of the current element, just insert the new
                    // interval and we're done; otherwise insert until the beginning of the
                    // current element
                    if (comparator.compare(incoming.end, current.start) <= 0)
                    {
                        list.add(i, incoming);
                        return;
                    }
                    list.add(i, new Range(incoming.start, current.start, incoming.markedAt, incoming.delTime));
                    i++;
                }

                // After that, we're overwritten on the current element but might have
                // some residual parts after ...

                // ... unless we don't extend beyond it.
                if (comparator.compare(incoming.end, current.end) <= 0)
                    return;

                incoming = new Range(current.end, incoming.end, incoming.markedAt, incoming.delTime);
                i++;
            }
        }

        // If we got there, then just insert the remainder at the end
        list.add(i, incoming);
    }

    public static class Serializer implements IVersionedSerializer<RangeTombstoneList>
    {
        private Serializer() {}

        public void serialize(RangeTombstoneList tombstones, DataOutput out, int version) throws IOException
        {
            if (tombstones == null)
            {
                out.writeInt(0);
                return;
            }

            out.writeInt(tombstones.size());
            for (Range range : tombstones.list)
            {
                ByteBufferUtil.writeWithShortLength(range.start, out);
                ByteBufferUtil.writeWithShortLength(range.end, out);
                out.writeInt(range.delTime);
                out.writeLong(range.markedAt);
            }
        }

        /*
         * RangeTombstoneList depends on the column family comparator, but it is not serialized.
         * Thus deserialize(DataInput, int, Comparator<ByteBuffer>) should be used instead of this method.
         */
        public RangeTombstoneList deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public RangeTombstoneList deserialize(DataInput in, int version, Comparator<ByteBuffer> comparator) throws IOException
        {
            int size = in.readInt();
            if (size == 0)
                return null;

            RangeTombstoneList tombstones = new RangeTombstoneList(comparator, size);

            for (int i = 0; i < size; i++)
            {
                ByteBuffer start = ByteBufferUtil.readWithShortLength(in);
                ByteBuffer end = ByteBufferUtil.readWithShortLength(in);
                int delTime =  in.readInt();
                long markedAt = in.readLong();

                if (version >= MessagingService.VERSION_20)
                {
                    tombstones.list.add(new Range(start, end, markedAt, delTime));
                }
                else
                {
                    /*
                     * The old implementation used to have range sorted by left value, but with potentially
                     * overlapping range. So we need to use the "slow" path.
                     */
                    tombstones.add(new Range(start, end, markedAt, delTime));
                }
            }

            return tombstones;
        }

        public long serializedSize(RangeTombstoneList tombstones, TypeSizes typeSizes, int version)
        {
            if (tombstones == null)
                return typeSizes.sizeof(0);

            long size = typeSizes.sizeof(tombstones.size());
            for (Range range : tombstones.list)
            {
                int startSize = range.start.remaining();
                size += typeSizes.sizeof((short)startSize) + startSize;
                int endSize = range.end.remaining();
                size += typeSizes.sizeof((short)endSize) + endSize;
                size += typeSizes.sizeof(range.delTime);
                size += typeSizes.sizeof(range.markedAt);
            }
            return size;
        }

        public long serializedSize(RangeTombstoneList tombstones, int version)
        {
            return serializedSize(tombstones, TypeSizes.NATIVE, version);
        }
    }

    /**
     * This object allow testing whether a given column (name/timestamp) is deleted
     * or not by this RangeTombstoneList, assuming that the column given to this
     * object are passed in (comparator) sorted order.
     *
     * This is more efficient that calling RangeTombstoneList.isDeleted() repeatedly
     * in that case since we're able to take the sorted nature of the RangeTombstoneList
     * into account.
     */
    public class InOrderTester
    {
        private int idx;

        public boolean isDeleted(ByteBuffer name, long timestamp)
        {
            while (idx < size())
            {
                Range current = list.get(idx);
                int cmp = comparator.compare(name, current.start);
                if (cmp == 0)
                {
                    // As for searchInternal, we need to check the previous end
                    Range last = idx > 0 ? list.get(idx -1) : null;
                    if (last != null && comparator.compare(name, last.end) == 0 && last.markedAt > current.markedAt)
                        return last.markedAt >= timestamp;
                    else
                        return current.markedAt >= timestamp;
                }
                else if (cmp < 0)
                {
                    return false;
                }
                else
                {
                    if (comparator.compare(name, current.end) <= 0)
                        return current.markedAt >= timestamp;
                    else
                        idx++;
                }
            }
            return false;
        }
    }

    class EndsComparator implements Comparator<Range>
    {
        public int compare(Range a, Range b)
        {
            return comparator.compare(a.end, b.end);
        }
    }

    class StartsComparator implements Comparator<Range>
    {
        public int compare(Range a, Range b)
        {
            return comparator.compare(a.start, b.start);
        }
    }

    static class Range
    {
        public ByteBuffer start;
        public ByteBuffer end;
        public long markedAt;
        public int delTime;

        Range(ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
        {
            this.start = start;
            this.end = end;
            this.markedAt = markedAt;
            this.delTime = delTime;
        }
    }
}
