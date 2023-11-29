/*
 * Copyright 2023 AntGroup CO., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.antgroup.geaflow.dsl.planner;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class GQLCostTest {
    @Test
    public void testGQLCostCalc() {
        GQLCost tiny = GQLCost.TINY;
        GQLCost huge = GQLCost.HUGE;
        GQLCost zero = GQLCost.ZERO;
        GQLCost infi = GQLCost.INFINITY;

        assertEquals(tiny.toString(), "{tiny}");
        assertEquals(huge.toString(), "{huge}");
        assertEquals(zero.toString(), "{0}");
        assertEquals(infi.toString(), "{inf}");

        assertEquals(tiny.hashCode(), 1138753536);
        assertEquals(huge.hashCode(), -1106247680);
        assertEquals(zero.hashCode(), 0);
        assertEquals(infi.hashCode(), 1106247680);

        assertFalse(zero.isEqWithEpsilon(tiny));
        assertFalse(tiny.isEqWithEpsilon(huge));
        assertFalse(huge.isEqWithEpsilon(infi));
        assertFalse(infi.isEqWithEpsilon(zero));

        assertEquals(zero.minus(tiny), tiny.minus(tiny).minus(tiny));
        assertTrue(tiny.minus(huge).isEqWithEpsilon(zero.minus(huge)));
        assertFalse(huge.minus(infi).isEqWithEpsilon(huge));
        assertEquals(infi.minus(zero), infi);

        assertEquals(zero.divideBy(tiny), 1.0);
        assertEquals(tiny.divideBy(huge), 0.0);
        assertEquals(huge.divideBy(infi), 1.0);
        assertEquals(infi.divideBy(zero), 1.0);

        assertFalse(zero.equals(tiny));
        assertFalse(tiny.equals(huge));
        assertFalse(huge.equals(infi));
        assertFalse(infi.equals(zero));

        assertFalse(zero.equals((Object)tiny));
        assertFalse(tiny.equals((Object)huge));
        assertFalse(huge.equals((Object)infi));
        assertFalse(infi.equals((Object)zero));

    }
}
