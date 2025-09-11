/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.vectorizedlookup;

import org.apache.calcite.sql.SqlFunction;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class VectorizedLookupOperatorConversionTest
{
  @Test
  public void testSqlFunctionDefinition()
  {
    LookupExtractorFactoryContainerProvider mockProvider = EasyMock.createMock(LookupExtractorFactoryContainerProvider.class);
    VectorizedLookupOperatorConversion conversion = new VectorizedLookupOperatorConversion(mockProvider);

    SqlFunction sqlFunction = conversion.calciteOperator();
    Assert.assertNotNull(sqlFunction);
    Assert.assertEquals("VECTORIZED_LOOKUP", sqlFunction.getName());
  }

  @Test
  public void testStaticSqlFunction()
  {
    SqlFunction sqlFunction = VectorizedLookupOperatorConversion.SQL_FUNCTION;
    Assert.assertNotNull(sqlFunction);
    Assert.assertEquals("VECTORIZED_LOOKUP", sqlFunction.getName());
  }
}
