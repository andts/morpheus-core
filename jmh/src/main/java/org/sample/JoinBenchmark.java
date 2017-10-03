/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sample;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.join.JoinTest;
import com.zavtech.morpheus.util.Resource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.math.BigInteger;

/**
 * java -jar target/benchmarks.jar JoinBenchmark
 */
@State(Scope.Benchmark)
public class JoinBenchmark {

    public DataFrame<BigInteger, String> venues;
    public DataFrame<BigInteger, String> events;

    @Setup
    public void setup() {
        venues = DataFrame.read().csv(options -> {
            options.setResource(Resource.of(this.getClass().getClassLoader().getResource("venue.csv")));
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });
        events = DataFrame.read().csv(options -> {
            options.setResource(Resource.of(this.getClass().getClassLoader().getResource("event.csv")));
            options.setRowKeyParser(BigInteger.class, row -> new BigInteger(row[0]));
        });
    }

    @Benchmark
    public DataFrame<BigInteger, String> testLoopJoin() {
        return JoinTest.loopJoin(events, venues,
            (left, right) -> left.getValue("venueid").equals(right.getValue("venueid")));
    }

    @Benchmark
    public DataFrame<BigInteger, String> testSortJoin() {
        return JoinTest.sortMergeJoin(venues, events, "venueid");
    }
}
