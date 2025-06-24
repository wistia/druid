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

package org.apache.druid.query.expression;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.utils.Sets;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LookupExprMacroTest extends MacroTestBase
{
  private static final LookupExtractor TEST_LOOKUP = new MapLookupExtractor(
      ImmutableMap.of("foo", "xfoo", "a", "xa", "abc", "xabc", "6", "x6"),
      false
  );

  public LookupExprMacroTest()
  {
    super(
        new LookupExprMacro(new LookupExtractorFactoryContainerProvider()
        {
          @Override
          public Set<String> getAllLookupNames()
          {
            return Sets.newHashSet("test_lookup");
          }

          @Override
          public Optional<LookupExtractorFactoryContainer> get(String lookupName)
          {
            if ("test_lookup".equals(lookupName)) {
              return Optional.of(new TestLookupContainer(TEST_LOOKUP));
            }
            return Optional.empty();
          }

          @Override
          public String getCanonicalLookupName(String lookupName)
          {
            return lookupName;
          }
        })
    );
  }

  @Test
  public void testTooFewArgs()
  {
    expectException(IllegalArgumentException.class, "Function[lookup] requires 2 to 3 arguments");
    apply(Collections.emptyList());
  }

  @Test
  public void testNonLiteralLookupName()
  {
    expectException(
        IllegalArgumentException.class,
        "Function[lookup] second argument must be a registered lookup name"
    );
    apply(getArgs(Lists.newArrayList("1", new ArrayList<String>())));
  }

  @Test
  public void testValidCalls()
  {
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("1", "test_lookup"))));
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("null", "test_lookup"))));
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("1", "test_lookup", null))));
    Assert.assertNotNull(apply(getArgs(Lists.newArrayList("1", "test_lookup", "N/A"))));
  }

  // Vectorization tests
  @Test
  public void testCanVectorize()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup")));

    // Test that lookup expressions can be vectorized when the input can be vectorized
    final Expr.InputBindingInspector inspector = InputBindings.inspectorFromTypeMap(
        ImmutableMap.of("x", ExpressionType.STRING)
    );
    Assert.assertTrue("Lookup expression should be vectorizable", expr.canVectorize(inspector));
  }

  @Test
  public void testCannotVectorizeWhenInputCannotVectorize()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup")));

    // Create a mock inspector that indicates the input cannot be vectorized
    final Expr.InputBindingInspector inspector = new Expr.InputBindingInspector()
    {
      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public boolean canVectorize(String name)
      {
        return false; // Input cannot be vectorized
      }
    };

    Assert.assertFalse("Lookup expression should not be vectorizable when input cannot be vectorized",
                       expr.canVectorize(inspector));
  }

  @Test
  public void testVectorProcessorCreation()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup")));

    final Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return 1024;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }
    };

    final ExprVectorProcessor<?> processor = expr.asVectorProcessor(inspector);
    Assert.assertNotNull("Vector processor should be created", processor);
    Assert.assertEquals("Output type should be STRING", ExpressionType.STRING, processor.getOutputType());
    Assert.assertEquals("Max vector size should match inspector", 1024, processor.maxVectorSize());
  }

  @Test
  public void testVectorEvaluation()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup")));

    // Create vector bindings with test data
    final Expr.VectorInputBinding vectorBinding = new Expr.VectorInputBinding()
    {
      private final String[] strings = {"foo", "a", "abc", "6"};
      private final int vectorSize = strings.length;

      @Override
      public Object[] getObjectVector(String name)
      {
        return strings;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public long[] getLongVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double[] getDoubleVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean[] getNullVector(String name)
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorId()
      {
        return 0;
      }
    };

    final Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return 4;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }
    };

    final ExprVectorProcessor<?> processor = expr.asVectorProcessor(inspector);
    final ExprEvalVector<?> result = processor.evalVector(vectorBinding);

    Assert.assertNotNull("Vector evaluation result should not be null", result);
    Assert.assertEquals("Result type should be STRING", ExpressionType.STRING, result.getType());

    final Object[] values = result.getObjectVector();
    Assert.assertEquals("Result should have 4 values", 4, values.length);

    // Verify lookup results based on the test lookup data
    Assert.assertEquals("lookup('foo', 'test_lookup') should return 'xfoo'", "xfoo", values[0]);
    Assert.assertEquals("lookup('a', 'test_lookup') should return 'xa'", "xa", values[1]);
    Assert.assertEquals("lookup('abc', 'test_lookup') should return 'xabc'", "xabc", values[2]);
    Assert.assertEquals("lookup('6', 'test_lookup') should return 'x6'", "x6", values[3]);
  }

  @Test
  public void testVectorEvaluationWithMissingValues()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup", "N/A")));

    // Create vector bindings with test data including missing values
    final Expr.VectorInputBinding vectorBinding = new Expr.VectorInputBinding()
    {
      private final String[] strings = {"foo", "missing_key", "abc", "another_missing"};
      private final int vectorSize = strings.length;

      @Override
      public Object[] getObjectVector(String name)
      {
        return strings;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public long[] getLongVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double[] getDoubleVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean[] getNullVector(String name)
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorId()
      {
        return 0;
      }
    };

    final Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return 4;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }
    };

    final ExprVectorProcessor<?> processor = expr.asVectorProcessor(inspector);
    final ExprEvalVector<?> result = processor.evalVector(vectorBinding);

    Assert.assertNotNull("Vector evaluation result should not be null", result);
    Assert.assertEquals("Result type should be STRING", ExpressionType.STRING, result.getType());

    final Object[] values = result.getObjectVector();
    Assert.assertEquals("Result should have 4 values", 4, values.length);

    // Verify lookup results with missing value handling
    Assert.assertEquals("lookup('foo', 'test_lookup', 'N/A') should return 'xfoo'", "xfoo", values[0]);
    Assert.assertEquals("lookup('missing_key', 'test_lookup', 'N/A') should return 'N/A'", "N/A", values[1]);
    Assert.assertEquals("lookup('abc', 'test_lookup', 'N/A') should return 'xabc'", "xabc", values[2]);
    Assert.assertEquals("lookup('another_missing', 'test_lookup', 'N/A') should return 'N/A'", "N/A", values[3]);
  }

  @Test
  public void testVectorEvaluationWithNullMissingValue()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup", null)));

    // Create vector bindings with test data including missing values
    final Expr.VectorInputBinding vectorBinding = new Expr.VectorInputBinding()
    {
      private final String[] strings = {"foo", "missing_key", "abc"};
      private final int vectorSize = strings.length;

      @Override
      public Object[] getObjectVector(String name)
      {
        return strings;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public long[] getLongVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double[] getDoubleVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean[] getNullVector(String name)
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorId()
      {
        return 0;
      }
    };

    final Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return 3;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }
    };

    final ExprVectorProcessor<?> processor = expr.asVectorProcessor(inspector);
    final ExprEvalVector<?> result = processor.evalVector(vectorBinding);

    Assert.assertNotNull("Vector evaluation result should not be null", result);
    Assert.assertEquals("Result type should be STRING", ExpressionType.STRING, result.getType());

    final Object[] values = result.getObjectVector();
    Assert.assertEquals("Result should have 3 values", 3, values.length);

    // Verify lookup results with null missing value handling
    Assert.assertEquals("lookup('foo', 'test_lookup', null) should return 'xfoo'", "xfoo", values[0]);
    Assert.assertNull("lookup('missing_key', 'test_lookup', null) should return null", values[1]);
    Assert.assertEquals("lookup('abc', 'test_lookup', null) should return 'xabc'", "xabc", values[2]);
  }

  @Test
  public void testVectorEvaluationConsistencyWithScalar()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup")));

    // Test data
    final String[] testInputs = {"foo", "a", "abc", "6"};
    final String[] expectedOutputs = {"xfoo", "xa", "xabc", "x6"};

    // Create vector bindings
    final Expr.VectorInputBinding vectorBinding = new Expr.VectorInputBinding()
    {
      @Override
      public Object[] getObjectVector(String name)
      {
        return testInputs;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public long[] getLongVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double[] getDoubleVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean[] getNullVector(String name)
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return testInputs.length;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return testInputs.length;
      }

      @Override
      public int getCurrentVectorId()
      {
        return 0;
      }
    };

    final Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return testInputs.length;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }
    };

    // Test vector evaluation
    final ExprVectorProcessor<?> processor = expr.asVectorProcessor(inspector);
    final ExprEvalVector<?> vectorResult = processor.evalVector(vectorBinding);
    final Object[] vectorValues = vectorResult.getObjectVector();

    // Test scalar evaluation for each input
    for (int i = 0; i < testInputs.length; i++) {
      final Expr.ObjectBinding scalarBinding = InputBindings.forInputSuppliers(
          ImmutableMap.of("x", InputBindings.inputSupplier(ExpressionType.STRING, () -> testInputs[i]))
      );
      final Object scalarResult = expr.eval(scalarBinding).value();

      Assert.assertEquals(
          String.format("Vector and scalar results should match for input '%s'", testInputs[i]),
          scalarResult,
          vectorValues[i]
      );
      Assert.assertEquals(
          String.format("Result should match expected for input '%s'", testInputs[i]),
          expectedOutputs[i],
          vectorValues[i]
      );
    }
  }

  @Test
  public void testVectorEvaluationWithEmptyVector()
  {
    final Expr expr = apply(getArgs(Lists.newArrayList("x", "test_lookup")));

    // Create vector bindings with empty data
    final Expr.VectorInputBinding vectorBinding = new Expr.VectorInputBinding()
    {
      private final String[] strings = {};
      private final int vectorSize = 0;

      @Override
      public Object[] getObjectVector(String name)
      {
        return strings;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public long[] getLongVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public double[] getDoubleVector(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean[] getNullVector(String name)
      {
        return null;
      }

      @Override
      public int getMaxVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorSize()
      {
        return vectorSize;
      }

      @Override
      public int getCurrentVectorId()
      {
        return 0;
      }
    };

    final Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public int getMaxVectorSize()
      {
        return 0;
      }

      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }
    };

    final ExprVectorProcessor<?> processor = expr.asVectorProcessor(inspector);
    final ExprEvalVector<?> result = processor.evalVector(vectorBinding);

    Assert.assertNotNull("Vector evaluation result should not be null", result);
    Assert.assertEquals("Result type should be STRING", ExpressionType.STRING, result.getType());

    final Object[] values = result.getObjectVector();
    Assert.assertEquals("Result should have 0 values", 0, values.length);
  }

  private List<Expr> getArgs(List<Object> args)
  {
    return args.stream().map(a -> {
      if (a != null && a instanceof String) {
        return ExprEval.of(a.toString()).toExpr();
      }
      return ExprEval.bestEffortOf(null).toExpr();
    }).collect(Collectors.toList());
  }

  private static class TestLookupContainer extends LookupExtractorFactoryContainer
  {
    public TestLookupContainer(final LookupExtractor theLookup)
    {
      super(
          "v0",
          new org.apache.druid.query.lookup.LookupExtractorFactory()
          {
            @Override
            public boolean start()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public boolean close()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public boolean replaces(org.apache.druid.query.lookup.LookupExtractorFactory other)
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public org.apache.druid.query.lookup.LookupIntrospectHandler getIntrospectHandler()
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public void awaitInitialization()
            {
            }

            @Override
            public boolean isInitialized()
            {
              return true;
            }

            @Override
            public LookupExtractor get()
            {
              return theLookup;
            }
          }
      );
    }
  }
}
