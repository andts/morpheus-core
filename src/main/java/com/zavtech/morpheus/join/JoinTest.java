package com.zavtech.morpheus.join;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumns;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameRow;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
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

    public static void main1(String[] args) {

        DataFrame<BigInteger, String> venues = DataFrame.read().csv(options -> {
            options.setResource("venue.csv");
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

//        DataFrame<BigInteger, String> categories = DataFrame.read().csv(options -> {
//            options.setResource("category.csv");
//            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
//        });

        DataFrame<BigInteger, String> events = DataFrame.read().csv(options -> {
            options.setResource("event.csv");
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

//        DataFrame<BigInteger, String> listings = DataFrame.read().csv(options -> {
//            options.setResource("listing.csv");
//            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
//        });

        long start1 = System.currentTimeMillis();
        DataFrame<BigInteger, String> joined11 = loopJoin(venues, events,
            (left, right) -> left.getValue("venueid").equals(right.getValue("venueid")));
        long end1 = System.currentTimeMillis();
        System.out.println("duration1() = " + (end1 - start1));

        long start2 = System.currentTimeMillis();
        DataFrame<BigInteger, String> joined12 = sortMergeJoinSimple(venues, events, "venueid");
        long end2 = System.currentTimeMillis();
        System.out.println("duration2() = " + (end2 - start2));

        joined11 = joined11.rows().sort(true, Arrays.asList("venueid", "eventid"));
        joined12 = joined12.rows().sort(true, Arrays.asList("venueid", "eventid"));

        joined11.out().print(500);
        System.out.println();
        System.out.println("joined11 = " + joined11.rowCount());
        joined12.out().print(500);
        System.out.println();
        System.out.println("joined12 = " + joined12.rowCount());

        System.out.println("equals = " + dfDataEquals(joined11, joined12));

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

    public static void main(String[] args) throws FileNotFoundException {

        DataFrame<BigInteger, String> testLeft = DataFrame.read().csv(options -> {
            options.setResource("venue.csv");
//            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

        DataFrame<BigInteger, String> testRight = DataFrame.read().csv(options -> {
            options.setResource("event.csv");
//            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });

        long start1 = System.currentTimeMillis();
        DataFrame<BigInteger, String> loopJoined = loopJoin(testLeft, testRight,
            (left, right) -> left.getValue("venueid").equals(right.getValue("venueid")));
        long end1 = System.currentTimeMillis();
        System.out.println("duration1() = " + (end1 - start1));

        long start2 = System.currentTimeMillis();
        DataFrame<BigInteger, String> sortJoined = sortMergeJoinFull(testLeft, testRight, "venueid");
        long end2 = System.currentTimeMillis();
        System.out.println("duration2() = " + (end2 - start2));

        long start3 = System.currentTimeMillis();
        DataFrame<BigInteger, String> sortSimpleJoined = sortMergeJoinSimple(testLeft, testRight, "venueid");
        long end3 = System.currentTimeMillis();
        System.out.println("duration3() = " + (end3 - start3));

        loopJoined = loopJoined.rows().sort(true, Arrays.asList("venueid", "eventid"));
        sortJoined = sortJoined.rows().sort(true, Arrays.asList("venueid", "eventid"));
        sortSimpleJoined = sortSimpleJoined.rows().sort(true, Arrays.asList("venueid", "eventid"));

        System.out.println("equals(sort, loop) = " + dfDataEquals(loopJoined, sortJoined));
        System.out.println("equals(sort1, sort2) = " + dfDataEquals(sortSimpleJoined, sortJoined));
//        System.out.println("");
//        System.out.println("Loop:");
//        loopJoined.out().print(500_000, new FileOutputStream("loopJoin.txt"));
//        System.out.println("");
//        System.out.println("Sort:");
//        sortJoined.out().print(500_000, new FileOutputStream("sortJoin.txt"));
    }

    private static boolean dfDataEquals(DataFrame<BigInteger, String> joined11,
                                        DataFrame<BigInteger, String> joined12) {
        for (int i = 0; i < joined11.rowCount(); ++i) {
            for (int j = 0; j < joined11.colCount(); ++j) {
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

    public static <C> DataFrame<BigInteger, C> sortMergeJoinSimple(DataFrame<BigInteger, C> left,
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

    public static <C> DataFrame<BigInteger, C> sortMergeJoinFull(DataFrame<BigInteger, C> left,
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
        int currentRightGroupStartIndex = 0;

        DataFrameCursor<BigInteger, C> leftCursor = sortedLeft.cursor();
        DataFrameCursor<BigInteger, C> rightCursor = sortedRight.cursor();

        DataFrameColumns<BigInteger, C> leftColumns = sortedLeft.cols();
        DataFrameColumns<BigInteger, C> rightColumns = sortedRight.cols();

        boolean dataAvailable = leftRowCount > 0 && rightRowCount > 0;
        boolean cursorMoved = false;

        while (dataAvailable) {
//            System.out.println(leftRowIndex + ", " + rightRowIndex);
            cursorMoved = false;

            leftCursor.moveToRow(leftRowIndex);
            rightCursor.moveToRow(rightRowIndex);

            Comparable leftVal = leftCursor.moveToColumn(joinColumn).getValue();
            Comparable rightVal = rightCursor.moveToColumn(joinColumn).getValue();
//            System.out.println("leftVal = " + leftVal);
//            System.out.println("rightVal = " + rightVal);
//            System.out.println("index = " + index);

            //if values are equal - join the rows
            if (leftVal.equals(rightVal)) {
                addJoinedRow(result, index, leftCursor, rightCursor, leftColumns, rightColumns);
                index = index.add(BigInteger.ONE);
            } else if (leftVal.compareTo(rightVal) < 0) {
                if (leftRowIndex + 1 < leftRowCount) {
                    leftRowIndex++;
                    cursorMoved = true;
//                    System.out.println(1);
                }
            } else if (leftVal.compareTo(rightVal) > 0) {
                if (rightRowIndex + 1 < rightRowCount) {
                    rightRowIndex++;
                    currentRightGroupStartIndex = rightRowIndex;
                    cursorMoved = true;
//                    System.out.println(2);
                }
            }

            //move the cursors to next positions
            if (!cursorMoved) {
                if (rightRowIndex + 1 < rightRowCount) {
                    int nextRowIndex = rightRowIndex + 1;
                    Comparable nextRightValue = sortedRight.data().getValue(nextRowIndex, joinColumn);
//                    System.out.println("nextRowIndex = " + nextRowIndex);
//                    System.out.println("nextRightValue = " + nextRightValue);
                    if (rightVal.equals(nextRightValue)) {
                        rightRowIndex++;
                        cursorMoved = true;
//                        System.out.println(3);
                    }
                }
            }

            if (!cursorMoved) {
                if (leftRowIndex + 1 < leftRowCount) {
                    Comparable nextLeftValue = sortedLeft.data().getValue(leftRowIndex + 1, joinColumn);
                    if (leftVal.equals(nextLeftValue)) {
                        leftRowIndex++;
                        rightRowIndex = currentRightGroupStartIndex;
                        cursorMoved = true;
//                        System.out.println(4);
                    } else {
                        //both next values are not same as current
                        //or there is no next right value
                        leftRowIndex++;
                        if (rightRowIndex + 1 < rightRowCount) {
                            rightRowIndex++;
                            currentRightGroupStartIndex = rightRowIndex;
//                            System.out.println(5);
                        }
                        cursorMoved = true;
//                        System.out.println(6);
                    }
                }
            }

            dataAvailable = cursorMoved;
        }

        return result;
    }

    private static <C> void addJoinedRow(DataFrame<BigInteger, C> result, BigInteger index,
                                         DataFrameCursor<BigInteger, C> leftCursor,
                                         DataFrameCursor<BigInteger, C> rightCursor,
                                         DataFrameColumns<BigInteger, C> leftColumns,
                                         DataFrameColumns<BigInteger, C> rightColumns) {
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
