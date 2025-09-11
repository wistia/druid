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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.compress.utils.Sets;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.math.expr.vector.ExprEvalVector;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupIntrospectHandler;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class LookupExprMacroTest extends InitializedNullHandlingTest
{
  private static final Expr.ObjectBinding BINDINGS = InputBindings.forInputSuppliers(
      ImmutableMap.<String, InputBindings.InputSupplier<?>>builder()
          .put("x", InputBindings.inputSupplier(ExpressionType.STRING, () -> "foo"))
          .put("y", InputBindings.inputSupplier(ExpressionType.STRING, () -> "bar"))
          .put("z", InputBindings.inputSupplier(ExpressionType.STRING, () -> "baz"))
          .build());

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  // Test setup for vectorized lookup macro
  private static final String TEST_LOOKUP_NAME = "test_lookup";
  private static final Map<String, String> TEST_LOOKUP_DATA = ImmutableMap.<String, String>builder()
      .put("foo", "xfoo")
      .put("bar", "xbar")
      .put("baz", "xbaz")
      .build();

  private final LookupExprMacro macro = new LookupExprMacro(createTestLookupProvider());

  @Test
  public void testTooFewArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Function[vectorized_lookup] requires 2 to 3 arguments");
    macro.apply(Collections.emptyList());
  }

  @Test
  public void testTooManyArgs()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Function[vectorized_lookup] requires 2 to 3 arguments");
    List<Expr> args = Lists.newArrayList(
        ExprEval.of("test").toExpr(),
        ExprEval.of(TEST_LOOKUP_NAME).toExpr(),
        ExprEval.of("default").toExpr(),
        ExprEval.of("extra").toExpr());
    macro.apply(args);
  }

  @Test
  public void testNonLiteralLookupName()
  {
    expectedException.expect(org.apache.druid.math.expr.ExpressionValidationException.class);
    expectedException.expectMessage("Function[vectorized_lookup] second argument argument must be a literal");
    List<Expr> args = Lists.newArrayList(
        ExprEval.of("test").toExpr(),
        Parser.parse("x + 1", ExprMacroTable.nil()) // Non-literal Expr
    );
    macro.apply(args);
  }

  @Test
  public void testValidCalls()
  {
    Assert.assertNotNull(macro.apply(getArgs(Lists.newArrayList("1", TEST_LOOKUP_NAME))));
    Assert.assertNotNull(macro.apply(getArgs(Lists.newArrayList("null", TEST_LOOKUP_NAME))));
    Assert.assertNotNull(macro.apply(getArgs(Lists.newArrayList("1", TEST_LOOKUP_NAME, null))));
    Assert.assertNotNull(macro.apply(getArgs(Lists.newArrayList("1", TEST_LOOKUP_NAME, "N/A"))));
  }

  @Test
  public void testBasicLookup()
  {
    assertExpr("vectorized_lookup(x, '" + TEST_LOOKUP_NAME + "')", "xfoo");
  }

  @Test
  public void testLookupMissingValue()
  {
    assertExpr("vectorized_lookup(y, '" + TEST_LOOKUP_NAME + "', 'N/A')", "xbar");
    assertExpr("vectorized_lookup('missing_key', '" + TEST_LOOKUP_NAME + "', 'N/A')", "N/A");
    assertExpr("vectorized_lookup('missing_key', '" + TEST_LOOKUP_NAME + "', null)", null);
  }

  @Test
  public void testLookupNotFound()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Lookup [nonexistent_lookup] not found");
    assertExpr("vectorized_lookup(x, 'nonexistent_lookup')", null);
  }

  @Test
  public void testCacheKeyChangesWhenLookupChanges()
  {
    final String expression = "vectorized_lookup(x, '" + TEST_LOOKUP_NAME + "')";
    final Expr expr = Parser.parse(expression, createTestExprMacroTable(TEST_LOOKUP_DATA));
    final Expr exprSameLookup = Parser.parse(expression, createTestExprMacroTable(TEST_LOOKUP_DATA));
    final Expr exprChangedLookup = Parser.parse(
        expression,
        createTestExprMacroTable(ImmutableMap.of("x", "y", "a", "b")));

    // same should have same cache key
    Assert.assertArrayEquals(expr.getCacheKey(), exprSameLookup.getCacheKey());

    // different should not have same key
    final byte[] exprBytes = expr.getCacheKey();
    final byte[] expr2Bytes = exprChangedLookup.getCacheKey();
    if (exprBytes.length == expr2Bytes.length) {
      // only check for equality if lengths are equal
      boolean allEqual = true;
      for (int i = 0; i < exprBytes.length; i++) {
        allEqual = allEqual && (exprBytes[i] == expr2Bytes[i]);
      }
      Assert.assertFalse(allEqual);
    }
  }

  @Test
  public void testCacheKeyChangesWhenLookupChangesSubExpr()
  {
    final String expression = "concat(vectorized_lookup(x, '" + TEST_LOOKUP_NAME + "'))";
    final Expr expr = Parser.parse(expression, createTestExprMacroTable(TEST_LOOKUP_DATA));
    final Expr exprSameLookup = Parser.parse(expression, createTestExprMacroTable(TEST_LOOKUP_DATA));
    final Expr exprChangedLookup = Parser.parse(
        expression,
        createTestExprMacroTable(ImmutableMap.of("x", "y", "a", "b")));

    // same should have same cache key
    Assert.assertArrayEquals(expr.getCacheKey(), exprSameLookup.getCacheKey());

    // different should not have same key
    final byte[] exprBytes = expr.getCacheKey();
    final byte[] expr2Bytes = exprChangedLookup.getCacheKey();
    if (exprBytes.length == expr2Bytes.length) {
      // only check for equality if lengths are equal
      boolean allEqual = true;
      for (int i = 0; i < exprBytes.length; i++) {
        allEqual = allEqual && (exprBytes[i] == expr2Bytes[i]);
      }
      Assert.assertFalse(allEqual);
    }
  }

  // Vectorization-specific tests
  @Test
  public void testCanVectorize()
  {
    List<Expr> args = Lists.newArrayList(
        ExprEval.of("test").toExpr(),
        ExprEval.of(TEST_LOOKUP_NAME).toExpr());
    Expr expr = macro.apply(args);

    // Test that the expression can vectorize
    Assert.assertTrue(expr.canVectorize(new TestInputBindingInspector()));
  }

  @Test
  public void testVectorProcessorCreation()
  {
    List<Expr> args = Lists.newArrayList(
        ExprEval.of("test").toExpr(),
        ExprEval.of(TEST_LOOKUP_NAME).toExpr());
    Expr expr = macro.apply(args);

    // Test that we can create a vector processor
    ExprVectorProcessor<Object[]> processor = expr.asVectorProcessor(new TestVectorInputBindingInspector());
    Assert.assertNotNull(processor);
    Assert.assertEquals(ExpressionType.STRING, processor.getOutputType());
    Assert.assertEquals(512, processor.maxVectorSize()); // Default max vector size
  }

  @Test
  public void testVectorProcessorEvaluation()
  {
    List<Expr> args = Lists.newArrayList(
        Parser.parse("x", ExprMacroTable.nil()), // Use identifier expression for "x"
        ExprEval.of(TEST_LOOKUP_NAME).toExpr());
    Expr expr = macro.apply(args);

    // Create a custom inspector that returns the actual data size
    Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public int getMaxVectorSize()
      {
        return 4; // Match the test data size
      }
    };

    ExprVectorProcessor<Object[]> processor = expr.asVectorProcessor(inspector);

    // Create test vector bindings
    TestVectorInputBinding bindings = new TestVectorInputBinding();
    bindings.put("x", new Object[]{"foo", "bar", "baz", "missing_key"});

    ExprEvalVector<Object[]> result = processor.evalVector(bindings);
    Object[] values = result.values();

    Assert.assertEquals(4, values.length);
    Assert.assertEquals("xfoo", values[0]);
    Assert.assertEquals("xbar", values[1]);
    Assert.assertEquals("xbaz", values[2]);
    Assert.assertEquals(null, values[3]); // missing key returns null
  }

  @Test
  public void testVectorProcessorWithDefaultValue()
  {
    List<Expr> args = Lists.newArrayList(
        Parser.parse("x", ExprMacroTable.nil()), // Use identifier expression for "x"
        ExprEval.of(TEST_LOOKUP_NAME).toExpr(),
        ExprEval.of("DEFAULT").toExpr());
    Expr expr = macro.apply(args);

    // Create a custom inspector that returns the actual data size
    Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public int getMaxVectorSize()
      {
        return 4; // Match the test data size
      }
    };

    ExprVectorProcessor<Object[]> processor = expr.asVectorProcessor(inspector);

    // Create test vector bindings
    TestVectorInputBinding bindings = new TestVectorInputBinding();
    bindings.put("x", new Object[]{"foo", "bar", "baz", "missing_key"});

    ExprEvalVector<Object[]> result = processor.evalVector(bindings);
    Object[] values = result.values();

    Assert.assertEquals(4, values.length);
    Assert.assertEquals("xfoo", values[0]);
    Assert.assertEquals("xbar", values[1]);
    Assert.assertEquals("xbaz", values[2]);
    Assert.assertEquals("DEFAULT", values[3]); // missing key returns default value
  }

  @Test
  public void testVectorProcessorBatchProcessing()
  {
    List<Expr> args = Lists.newArrayList(
        Parser.parse("x", ExprMacroTable.nil()), // Use identifier expression for "x"
        ExprEval.of(TEST_LOOKUP_NAME).toExpr());
    Expr expr = macro.apply(args);

    // Create a custom inspector that returns the actual data size
    Expr.VectorInputBindingInspector inspector = new Expr.VectorInputBindingInspector()
    {
      @Override
      public ExpressionType getType(String name)
      {
        return ExpressionType.STRING;
      }

      @Override
      public int getMaxVectorSize()
      {
        return 100; // Match the test data size
      }
    };

    ExprVectorProcessor<Object[]> processor = expr.asVectorProcessor(inspector);

    // Create test vector bindings with 100 identical values
    TestVectorInputBinding bindings = new TestVectorInputBinding();
    Object[] inputValues = new Object[100];
    for (int i = 0; i < 100; i++) {
      inputValues[i] = "foo";
    }
    bindings.put("x", inputValues);

    ExprEvalVector<Object[]> result = processor.evalVector(bindings);
    Object[] values = result.values();

    Assert.assertEquals(100, values.length);
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals("xfoo", values[i]);
    }
  }

  // Helper methods
  private void assertExpr(final String expression, final Object expectedResult)
  {
    final Expr expr = Parser.parse(expression, createTestExprMacroTable(TEST_LOOKUP_DATA));
    Assert.assertEquals(expression, expectedResult, expr.eval(BINDINGS).value());

    final Expr exprNotFlattened = Parser.parse(expression, createTestExprMacroTable(TEST_LOOKUP_DATA), false);
    final Expr roundTripNotFlattened = Parser.parse(exprNotFlattened.stringify(),
        createTestExprMacroTable(TEST_LOOKUP_DATA));
    Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTripNotFlattened.eval(BINDINGS).value());

    final Expr roundTrip = Parser.parse(expr.stringify(), createTestExprMacroTable(TEST_LOOKUP_DATA));
    Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTrip.eval(BINDINGS).value());
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

  private LookupExtractorFactoryContainerProvider createTestLookupProvider()
  {
    return new LookupExtractorFactoryContainerProvider()
    {
      @Override
      public Set<String> getAllLookupNames()
      {
        return Sets.newHashSet(TEST_LOOKUP_NAME);
      }

      @Override
      public Optional<LookupExtractorFactoryContainer> get(String lookupName)
      {
        if (TEST_LOOKUP_NAME.equals(lookupName)) {
          return Optional.of(new TestLookupContainer(new MapLookupExtractor(TEST_LOOKUP_DATA, false)));
        }
        return Optional.empty();
      }

      @Override
      public String getCanonicalLookupName(String lookupName)
      {
        return lookupName;
      }
    };
  }

  private ExprMacroTable createTestExprMacroTable(final Map<String, String> lookupData)
  {
    return new ExprMacroTable(Collections.singletonList(
        new LookupExprMacro(createTestLookupProviderWithData(lookupData))));
  }

  private LookupExtractorFactoryContainerProvider createTestLookupProviderWithData(
      final Map<String, String> lookupData)
  {
    return new LookupExtractorFactoryContainerProvider()
    {
      @Override
      public Set<String> getAllLookupNames()
      {
        return Sets.newHashSet(TEST_LOOKUP_NAME);
      }

      @Override
      public Optional<LookupExtractorFactoryContainer> get(String lookupName)
      {
        if (TEST_LOOKUP_NAME.equals(lookupName)) {
          return Optional.of(new TestLookupContainer(new MapLookupExtractor(lookupData, false)));
        }
        return Optional.empty();
      }

      @Override
      public String getCanonicalLookupName(String lookupName)
      {
        return lookupName;
      }
    };
  }

  // Test helper classes
  private static class TestLookupContainer extends LookupExtractorFactoryContainer
  {
    public TestLookupContainer(final LookupExtractor theLookup)
    {
      super(
          "v0",
          new LookupExtractorFactory()
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
            public boolean replaces(@Nullable final LookupExtractorFactory other)
            {
              throw new UnsupportedOperationException();
            }

            @Override
            public LookupIntrospectHandler getIntrospectHandler()
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
          });
    }
  }

  private static class TestInputBindingInspector implements Expr.InputBindingInspector
  {
    @Override
    public ExpressionType getType(String name)
    {
      return ExpressionType.STRING;
    }
  }

  private static class TestVectorInputBindingInspector implements Expr.VectorInputBindingInspector
  {
    @Override
    public ExpressionType getType(String name)
    {
      return ExpressionType.STRING;
    }

    @Override
    public int getMaxVectorSize()
    {
      return 512;
    }
  }

  private static class TestVectorInputBinding implements Expr.VectorInputBinding
  {
    private final Map<String, Object[]> bindings = new HashMap<>();

    public void put(String name, Object[] values)
    {
      bindings.put(name, values);
    }

    @Override
    public Object[] getObjectVector(String name)
    {
      return bindings.get(name);
    }

    @Override
    public int getCurrentVectorSize()
    {
      return bindings.values().iterator().next().length;
    }

    @Override
    public boolean[] getNullVector(String name)
    {
      Object[] vec = bindings.get(name);
      boolean[] nulls = new boolean[vec != null ? vec.length : 0];
      return nulls;
    }

    @Override
    public double[] getDoubleVector(String name)
    {
      Object[] vec = bindings.get(name);
      double[] arr = new double[vec != null ? vec.length : 0];
      return arr;
    }

    @Override
    public long[] getLongVector(String name)
    {
      Object[] vec = bindings.get(name);
      long[] arr = new long[vec != null ? vec.length : 0];
      return arr;
    }

    @Override
    public int getMaxVectorSize()
    {
      if (bindings.isEmpty()) {
        return 512;
      }
      return bindings.values().iterator().next().length;
    }

    @Override
    public ExpressionType getType(String name)
    {
      return ExpressionType.STRING;
    }

    @Override
    public int getCurrentVectorId()
    {
      return 0;
    }
  }
}
