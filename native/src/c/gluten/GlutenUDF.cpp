// Filename MyUDF.cpp

#include <velox/expression/VectorFunction.h>
#include <velox/udf/Udf.h>
#include <iostream>


namespace {
using namespace facebook::velox;

template <TypeKind Kind>
class PlusConstantFunction : public exec::VectorFunction {
 public:
  explicit PlusConstantFunction(int32_t addition) : addition_(addition) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    using nativeType = typename TypeTraits<Kind>::NativeType;
    VELOX_CHECK_EQ(args.size(), 1);

    auto& arg = args[0];

    // The argument may be flat or constant.
    VELOX_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());

    BaseVector::ensureWritable(rows, createScalarType<Kind>(), context.pool(), result);

    auto* flatResult = result->asFlatVector<nativeType>();
    auto* rawResult = flatResult->mutableRawValues();

    flatResult->clearNulls(rows);

    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<nativeType>>()->valueAt(0);
      rows.applyToSelected([&](auto row) { rawResult[row] = value + addition_; });
    } else {
      auto* rawInput = arg->as<FlatVector<nativeType>>()->rawValues();

      rows.applyToSelected([&](auto row) { rawResult[row] = rawInput[row] + addition_; });
    }
  }

 private:
  const int32_t addition_;
};

static std::vector<std::shared_ptr<exec::FunctionSignature>> integerSignatures() {
  // integer -> integer
  return {exec::FunctionSignatureBuilder().returnType("integer").argumentType("integer").build()};
}

static std::vector<std::shared_ptr<exec::FunctionSignature>> bigintSignatures() {
  // bigint -> bigint
  return {exec::FunctionSignatureBuilder().returnType("bigint").argumentType("bigint").build()};
}

} // namespace

const int kNumMyUdf = 2;
gluten::UdfEntry myUdf[kNumMyUdf] = {{"myudf1", "integer"}, {"myudf2", "bigint"}};

DEFINE_GET_NUM_UDF {
  return kNumMyUdf;
}

DEFINE_GET_UDF_ENTRIES {
  for (auto i = 0; i < kNumMyUdf; ++i) {
    udfEntries[i] = myUdf[i];
  }
}

DEFINE_REGISTER_UDF {
  facebook::velox::exec::registerVectorFunction(
      "myudf1", integerSignatures(), std::make_unique<PlusConstantFunction<facebook::velox::TypeKind::INTEGER>>(5));
  facebook::velox::exec::registerVectorFunction(
      "myudf2", bigintSignatures(), std::make_unique<PlusConstantFunction<facebook::velox::TypeKind::BIGINT>>(5));
  std::cout << "registered myudf1, myudf2" << std::endl;
}
