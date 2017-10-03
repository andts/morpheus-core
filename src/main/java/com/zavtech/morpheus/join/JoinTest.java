package com.zavtech.morpheus.join;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameRow;

import java.math.BigInteger;
import java.util.Arrays;

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

        long start1 = System.currentTimeMillis();
        DataFrame<BigInteger, String> joined11 = loopJoin(venues, events,
            (left, right) -> left.getValue("venueid").equals(right.getValue("venueid")));
        long end1 = System.currentTimeMillis();
        System.out.println("duration1() = " + (end1 - start1));

        long start2 = System.currentTimeMillis();
        DataFrame<BigInteger, String> joined12 = sortMergeJoin(venues, events, "venueid");
        long end2 = System.currentTimeMillis();
        System.out.println("duration2() = " + (end2 - start2));

        String[] cols = joined11.cols().keys().skip(1).toArray(String[]::new);

//        soutv
        joined11 = joined11.rows().sort(true, Arrays.asList("venueid", "eventid"));
        joined12 = joined12.rows().sort(true, Arrays.asList("venueid", "eventid"));

        joined11.out().print(500);
        System.out.println();
        System.out.println("joined11 = " + joined11.rowCount());
        joined12.out().print(500);
        System.out.println();
        System.out.println("joined12 = " + joined12.rowCount());

        System.out.println("equals = " + dfEquals(joined11, joined12));

//
//        DataFrame<BigInteger, String> joined2 = loopJoin(joined11, categories,
//            (left, right) -> left.getValue("catid").equals(right.getValue("catid")));
////        joined2.out().print(500);
////        System.out.println();
//
//        long start = System.currentTimeMillis();
//        System.out.println("start1() = " + start1);
//        DataFrame<BigInteger, String> joined31 = loopJoin(listings, joined2,
//            (left, right) -> left.getValue("eventid").equals(right.getValue("eventid")));
//        long end = System.currentTimeMillis();
//        System.out.println("duration1() = " + (end - start));
//
////        long start2 = System.currentTimeMillis();
////        System.out.println("start2() = " + start2);
////        DataFrame<BigInteger, String> joined32 = sortMergeJoin(listings, joined2, "eventid");
////        long end2 = System.currentTimeMillis();
////        System.out.println("duration2() = " + (end2 - start2));
//
//        joined31.out().print(500);
//        System.out.println();
//        System.out.println();
////        joined32.out().print(500);
////        System.out.println();
////        System.out.println();
//
//        System.out.println(venues.rows().count());
//        System.out.println(events.rows().count());
//        System.out.println(listings.rows().count());
//        System.out.println(joined11.rows().count());
//        System.out.println(joined2.rows().count());
//        System.out.println(joined3.rows().count());
    }

    private static boolean dfEquals(DataFrame<BigInteger, String> joined11, DataFrame<BigInteger, String> joined12) {
        for (int i=0; i<joined11.rowCount(); ++i) {
            for (int j=0; j<joined11.colCount(); ++j) {
                final Object value1 = joined11.data().getValue(i, j);
                final Object value2 = joined12.data().getValue(i, j);
                if (value1 == null && value2 != null) {
                    return false;
                } else if (value1 != null && !value1.equals(value2)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static <C> DataFrame<BigInteger, C> loopJoin(DataFrame<BigInteger, C> left,
                                                        DataFrame<BigInteger, C> right,
                                                        JoinPredicate<C> joinPredicate) {
        DataFrame<BigInteger, C> result = DataFrame.empty();
        left.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));
        right.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));

        DataFrameCursor<BigInteger, C> leftCursor = left.cursor();
        DataFrameCursor<BigInteger, C> rightCursor = right.cursor();
        BigInteger index = BigInteger.ZERO;

        int leftRowIndex = 0;
        int leftRowCount = left.rowCount();

        int rightRowIndex = 0;
        int rightRowCount = right.rowCount();

        DataFrameColumns<BigInteger, C> leftColumns = left.cols();
        DataFrameColumns<BigInteger, C> rightColumns = right.cols();

        while (leftRowIndex < leftRowCount) {
            leftCursor.moveToRow(leftRowIndex);
            DataFrameRow<BigInteger, C> leftRow = leftCursor.row();
            rightRowIndex = 0;
            while (rightRowIndex < rightRowCount) {
                rightCursor.moveToRow(rightRowIndex);
                DataFrameRow<BigInteger, C> rightRow = rightCursor.row();

                if (joinPredicate.test(leftRow, rightRow)) {
                    result.rows().add(index,
                        value -> {
                            if (leftColumns.contains(value.colKey())) {
                                return leftCursor.moveToColumn(value.colKey()).getValue();
                            } else if (rightColumns.contains(value.colKey())) {
                                return rightCursor.moveToColumn(value.colKey()).getValue();
                            } else {
                                throw new IllegalStateException();
                            }
                        });
                    index = index.add(BigInteger.ONE);
                }
                rightRowIndex++;
            }
            leftRowIndex++;
        }
        return result;
    }

    public static <C> DataFrame<BigInteger, C> sortMergeJoin(DataFrame<BigInteger, C> left,
                                                             DataFrame<BigInteger, C> right,
                                                             C joinColumn) {
        DataFrame<BigInteger, C> sortedLeft = copyAndSort(left, joinColumn);
        DataFrame<BigInteger, C> sortedRight = copyAndSort(right, joinColumn);
        DataFrame<BigInteger, C> result = DataFrame.empty();
        left.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));
        right.cols().forEach(col -> result.cols().add(col.key(), col.typeInfo()));

        BigInteger index = BigInteger.ZERO;

        int leftRowIndex = 0;
        int leftRowCount = sortedLeft.rowCount();

        int rightRowIndex = 0;
        int rightRowCount = sortedRight.rowCount();

        DataFrameCursor<BigInteger, C> leftCursor = sortedLeft.cursor();
        DataFrameCursor<BigInteger, C> rightCursor = sortedRight.cursor();

        DataFrameColumns<BigInteger, C> leftColumns = sortedLeft.cols();
        DataFrameColumns<BigInteger, C> rightColumns = sortedRight.cols();

        while (leftRowIndex < leftRowCount && rightRowIndex < rightRowCount) {
            leftCursor.moveToRow(leftRowIndex);
            rightCursor.moveToRow(rightRowIndex);

            Comparable leftVal = leftCursor.moveToColumn(joinColumn).getValue();
            Comparable rightVal = rightCursor.moveToColumn(joinColumn).getValue();

            if (leftVal.compareTo(rightVal) == 0) {
                result.rows().add(index,
                    value -> {
                        if (leftColumns.contains(value.colKey())) {
                            return leftCursor.moveToColumn(value.colKey()).getValue();
                        } else if (rightColumns.contains(value.colKey())) {
                            return rightCursor.moveToColumn(value.colKey()).getValue();
                        } else {
                            throw new IllegalStateException();
                        }
                    });
                index = index.add(BigInteger.ONE);
                rightRowIndex++;
            } else if (leftVal.compareTo(rightVal) < 0) {
                leftRowIndex++;
            } else if (leftVal.compareTo(rightVal) > 0) {
                rightRowIndex++;
            }
        }

        return result;
    }

    private static <C> DataFrame<BigInteger, C> copyAndSort(DataFrame<BigInteger, C> dataFrame, C sortColumn) {
        return dataFrame.copy().rows().parallel().sort(true, sortColumn);
    }

    @FunctionalInterface
    public interface JoinPredicate<T> {
        boolean test(DataFrameRow<BigInteger, T> leftRow, DataFrameRow<BigInteger, T> rightRow);
    }

    public enum JoinType {
        INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, CROSS
    }
}
