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

package org.apache.cassandra.index.sai.metrics;

// copied from codahale metrics RatioGauge::Ratio but with numerator and denominator visible,
// which allows us to compose two Ratios of the same type together
public class Ratio {
    public final double numerator;
    public final double denominator;

    public static Ratio of(double numerator, double denominator) {
        return new Ratio(numerator, denominator);
    }

    private Ratio(double numerator, double denominator) {
        this.numerator = numerator;
        this.denominator = denominator;
    }

    public double getValue() {
        double d = this.denominator;
        return !Double.isNaN(d) && !Double.isInfinite(d) && d != 0.0 ? this.numerator / d : Double.NaN;
    }

    public String toString() {
        return this.numerator + ":" + this.denominator;
    }
}

