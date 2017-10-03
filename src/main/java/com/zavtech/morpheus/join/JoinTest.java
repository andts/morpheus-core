package com.zavtech.morpheus.join;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameRow;

import java.math.BigInteger;

/**
 * 1. Implement all known join algorithm.
 * 2. Specify constraints for each algo to be able to decide when each one can be used.
 * 3. Decide which one will be the fallback algo (nested loop probably).
 * 4. Create some join builder (mimic sql syntax?), that accepts dataframes to join, then conditions. Initial impl uses fallback algo for all joins.
 * 5. Implement join order optimization based on metadata (use same join algo).
 * 6. Implement join algo selection based on table metadata.
 */

public class JoinTest {

    public static void main(String[] args) {

        DataFrame<BigInteger, String> venues = DataFrame.read().csv(options -> {
            options.setResource("venue.csv");
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

        DataFrame<BigInteger, String> categories = DataFrame.read().csv(options -> {
            options.setResource("category.csv");
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

        DataFrame<BigInteger, String> events = DataFrame.read().csv(options -> {
            options.setResource("event.csv");
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

        DataFrame<BigInteger, String> listings = DataFrame.read().csv(options -> {
            options.setResource("listing.csv");
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

        DataFrame<BigInteger, String> joined = loopJoin(events, venues,
            (left, right) -> left.getValue("venueid").equals(right.getValue("venueid")));

        joined.out().print(500);
        System.out.println();

        DataFrame<BigInteger, String> joined2 = loopJoin(joined, categories,
            (left, right) -> left.getValue("catid").equals(right.getValue("catid")));
        joined2.out().print(500);
        System.out.println();

        long start = System.currentTimeMillis();
        System.out.println("start() = " + start);
        DataFrame<BigInteger, String> joined3 = loopJoin(listings, joined2,
            (left, right) -> left.getValue("eventid").equals(right.getValue("eventid")));
        long end = System.currentTimeMillis();
        System.out.println("duration() = " + (end - start));
        joined3.out().print(500);
        System.out.println();

        System.out.println(venues.rows().count());
        System.out.println(events.rows().count());
        System.out.println(listings.rows().count());
        System.out.println(joined.rows().count());
        System.out.println(joined2.rows().count());
        System.out.println(joined3.rows().count());
    }

    private static <C> DataFrame<BigInteger, C> loopJoin(DataFrame<BigInteger, C> left,
                                                         DataFrame<BigInteger, C> right,
                                                         JoinPredicate<C> joinPredicate) {
        DataFrame<BigInteger, C> result = DataFrame.empty();
        left.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));
        right.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));

        BigInteger index = BigInteger.ZERO;
        for (DataFrameRow<BigInteger, C> leftRow : left.rows()) {
            for (DataFrameRow<BigInteger, C> rightRow : right.rows()) {
                if (joinPredicate.test(leftRow, rightRow)) {
                    result.rows().add(index,
                        value -> {
                            if (left.cols().contains(value.colKey())) {
                                return leftRow.getValue(value.colKey());
                            } else if (right.cols().contains(value.colKey())) {
                                return rightRow.getValue(value.colKey());
                            } else {
                                throw new IllegalStateException();
                            }
                        });
                    index = index.add(BigInteger.ONE);
                }
            }
        }

        return result;
    }

    private static <C> DataFrame<BigInteger, C> sortMergeJoin(DataFrame<BigInteger, C> left,
                                                              DataFrame<BigInteger, C> right,
                                                              C joinColumn) {
        DataFrame<BigInteger, C> sortedLeft = copyAndSort(left, joinColumn);
        DataFrame<BigInteger, C> sortedRight = copyAndSort(right, joinColumn);
        DataFrame<BigInteger, C> result = DataFrame.empty();
        left.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));
        right.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));

        BigInteger index = BigInteger.ZERO;
        for (DataFrameRow<BigInteger, C> leftRow : left.rows()) {
            for (DataFrameRow<BigInteger, C> rightRow : right.rows()) {
                if (joinPredicate.test(leftRow, rightRow)) {
                    result.rows().add(index,
                        value -> {
                            if (left.cols().contains(value.colKey())) {
                                return leftRow.getValue(value.colKey());
                            } else if (right.cols().contains(value.colKey())) {
                                return rightRow.getValue(value.colKey());
                            } else {
                                throw new IllegalStateException();
                            }
                        });
                    index = index.add(BigInteger.ONE);
                }
            }
        }

        return result;
    }

    private static <C> DataFrame<BigInteger, C> copyAndSort(DataFrame<BigInteger, C> dataFrame, C sortColumn) {
        return dataFrame.copy().rows().parallel().sort(true, sortColumn);
    }

    @FunctionalInterface
    private interface JoinPredicate<T> {
        boolean test(DataFrameRow<BigInteger, T> leftRow, DataFrameRow<BigInteger, T> rightRow);
    }

    private enum JoinType {
        INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, CROSS
    }
}
