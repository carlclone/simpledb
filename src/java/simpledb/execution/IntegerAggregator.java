package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final Object aggr;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield; //group by field
        this.gbfieldtype = gbfieldtype; // goup by field type
        this.afield = afield;  // aggr field
        this.what = what; // count , min , max ....

        if (this.gbfield == Aggregator.NO_GROUPING) {
            aggr = (Object) new ArrayList<Integer>();
        } else {
            assert gbfieldtype != null;
            if (gbfieldtype == Type.INT_TYPE) {
                aggr = (Object) new TreeMap<Integer, ArrayList<Integer>>();
            } else {
                aggr = (Object) new TreeMap<String, ArrayList<Integer>>();

            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.gbfield == Aggregator.NO_GROUPING) {
            ((ArrayList<Integer>) aggr).add(((IntField)tup.getField(afield)).getValue());
        } else {
            if (gbfieldtype == Type.INT_TYPE) {
                TreeMap<Integer,ArrayList<Integer>> groupAggr = (TreeMap<Integer, ArrayList<Integer>>) aggr;
                Integer gbKey = ((IntField) tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField) tup.getField(afield)).getValue();

                if (!groupAggr.containsKey(gbKey)) {
                    groupAggr.put(gbKey, new ArrayList<>(1));
                }
                groupAggr.get(gbKey).add(aggrVal);
            } else if (gbfieldtype == Type.STRING_TYPE) {
                TreeMap<String, ArrayList<Integer>> groupAggr = (TreeMap<String, ArrayList<Integer>>) aggr;
                String gbKey = ((StringField) tup.getField(gbfield)).getValue();
                Integer aggrVal = ((IntField) tup.getField(afield)).getValue();

                if (!groupAggr.containsKey(gbKey)) {
                    groupAggr.put(gbKey, new ArrayList<>(1));
                }
                groupAggr.get(gbKey).add(aggrVal);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggrDbIterator();
    }

    private class AggrDbIterator implements OpIterator {
        private ArrayList<Tuple> res;
        private Iterator<Tuple> it;

        public AggrDbIterator() {
            res = new ArrayList<Tuple>();
            if (gbfield == Aggregator.NO_GROUPING) {
                Tuple t = new Tuple(getTupleDesc());
                Field aggregateVal = new IntField(this.calcAggrRes((ArrayList<Integer>) aggr));
                t.setField(0,aggregateVal);
                res.add(t);
            } else {
                for (Map.Entry e : ((TreeMap<Integer, ArrayList<Integer>>) aggr).entrySet()) {
                    Tuple t = new Tuple(getTupleDesc());
                    Field groupVal = null;
                    if (gbfieldtype == Type.INT_TYPE) {
                        groupVal = new IntField((int) e.getKey());
                    } else {
                        String str = (String) e.getKey();
                        groupVal = new StringField(str, str.length());
                    }
                    Field aggregateVal = new IntField(this.calcAggrRes((ArrayList<Integer>) e.getValue()));
                    t.setField(0, groupVal);
                    t.setField(1, aggregateVal);
                    res.add(t);
                }
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = res.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("not open");
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (it == null) {
                throw new IllegalStateException("IntegerAggregator not open");
            }
            it = res.iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            if (gbfield == Aggregator.NO_GROUPING) {
                return new TupleDesc(new Type[]{Type.INT_TYPE});
            } else {
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }
        }

        @Override
        public void close() {
            it = null;
        }

        //helper
        public int calcAggrRes(ArrayList<Integer> l) {
            assert !l.isEmpty();
            int res = 0;
            switch (what) {
                case MIN:
                    res = l.get(0);
                    for (int v : l) {
                        if (res > v) {
                            res = v;
                        }
                    }
                    break;
                case MAX:
                    res = l.get(0);
                    for (int v : l) {
                        if (res < v) {
                            res = v;
                        }
                    }
                    break;
                case SUM:
                    res = 0;
                    for (int v : l) {
                        res += v;
                    }
                    break;
                case AVG:
                    res = 0;
                    for (int v : l) {
                        res += v;
                    }
                    res = res / l.size();
                    break;
                case COUNT:
                    res = l.size();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            return res;
        }
    }
}
