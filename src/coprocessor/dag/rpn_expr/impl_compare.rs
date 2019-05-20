// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::{codec, Error, Result};

#[derive(RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnCompare<C: Comparer> {
    _phantom: std::marker::PhantomData<C>,
}

impl<C: Comparer> RpnFnCompare<C> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        lhs: &Option<C::T>,
        rhs: &Option<C::T>,
    ) -> Result<Option<i64>> {
        C::compare(lhs, rhs)
    }
}

impl<C: Comparer> std::fmt::Debug for RpnFnCompare<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RpnFnCompare")
    }
}

// See rust-lang/rust#26925 for why the followings are implemented manually. =====

impl<C: Comparer> Copy for RpnFnCompare<C> {}

impl<C: Comparer> Clone for RpnFnCompare<C> {
    fn clone(&self) -> Self {
        Self::new()
    }
}

// ======

pub trait Comparer: 'static + Send + Sync {
    type T: Evaluable;

    fn compare(lhs: &Option<Self::T>, rhs: &Option<Self::T>) -> Result<Option<i64>>;
}

pub struct BasicComparer<T: Evaluable + Ord, F: CmpOp> {
    _phantom_t: std::marker::PhantomData<T>,
    _phantom_f: std::marker::PhantomData<F>,
}

impl<T: Evaluable + Ord, F: CmpOp> Comparer for BasicComparer<T, F> {
    type T = T;

    fn compare(lhs: &Option<T>, rhs: &Option<T>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => Some(F::compare_order(lhs.cmp(rhs)) as i64),
        })
    }
}

pub struct RealComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for RealComparer<F> {
    type T = Real;

    fn compare(lhs: &Option<Real>, rhs: &Option<Real>) -> Result<Option<i64>> {
        match (lhs, rhs) {
            (None, None) => Ok(F::compare_null()),
            (None, _) | (_, None) => Ok(F::compare_partial_null()),
            (Some(lhs), Some(rhs)) => lhs
                .partial_cmp(rhs)
                // FIXME: It is weird to be a codec error.
                // FIXME: This should never happen because special numbers like NaN and Inf are not
                // allowed at all.
                .ok_or_else(|| {
                    Error::from(codec::Error::InvalidDataType(format!(
                        "{} and {} can't be compared",
                        lhs, rhs
                    )))
                })
                .map(|v| Some(F::compare_order(v) as i64)),
        }
    }
}

pub struct UintUintComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintUintComparer<F> {
    type T = Int;

    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let lhs = *lhs as u64;
                let rhs = *rhs as u64;
                Some(F::compare_order(lhs.cmp(&rhs)) as i64)
            }
        })
    }
}

pub struct UintIntComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for UintIntComparer<F> {
    type T = Int;

    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *rhs < 0 || *lhs as u64 > std::i64::MAX as u64 {
                    Ordering::Greater
                } else {
                    lhs.cmp(&rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub struct IntUintComparer<F: CmpOp> {
    _phantom_f: std::marker::PhantomData<F>,
}

impl<F: CmpOp> Comparer for IntUintComparer<F> {
    type T = Int;

    fn compare(lhs: &Option<Int>, rhs: &Option<Int>) -> Result<Option<i64>> {
        Ok(match (lhs, rhs) {
            (None, None) => F::compare_null(),
            (None, _) | (_, None) => F::compare_partial_null(),
            (Some(lhs), Some(rhs)) => {
                let ordering = if *lhs < 0 || *rhs as u64 > std::i64::MAX as u64 {
                    Ordering::Less
                } else {
                    lhs.cmp(&rhs)
                };
                Some(F::compare_order(ordering) as i64)
            }
        })
    }
}

pub trait CmpOp: 'static + Send + Sync {
    fn compare_null() -> Option<i64> {
        None
    }

    fn compare_partial_null() -> Option<i64> {
        None
    }

    fn compare_order(ordering: std::cmp::Ordering) -> bool;
}

pub struct CmpOpLT;

impl CmpOp for CmpOpLT {
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Less
    }
}

pub struct CmpOpLE;

impl CmpOp for CmpOpLE {
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Greater
    }
}

pub struct CmpOpGT;

impl CmpOp for CmpOpGT {
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Greater
    }
}

pub struct CmpOpGE;

impl CmpOp for CmpOpGE {
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Less
    }
}

pub struct CmpOpNE;

impl CmpOp for CmpOpNE {
    fn compare_order(ordering: Ordering) -> bool {
        ordering != Ordering::Equal
    }
}

pub struct CmpOpEQ;

impl CmpOp for CmpOpEQ {
    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Equal
    }
}

pub struct CmpOpNullEQ;

impl CmpOp for CmpOpNullEQ {
    fn compare_null() -> Option<i64> {
        Some(1)
    }

    fn compare_partial_null() -> Option<i64> {
        Some(0)
    }

    fn compare_order(ordering: Ordering) -> bool {
        ordering == Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::builder::FieldTypeBuilder;
    use cop_datatype::{FieldTypeFlag, FieldTypeTp};
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::rpn_expr::types::test_util::RpnFnScalarEvaluator;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum TestCaseCmpOp {
        GT,
        GE,
        LT,
        LE,
        EQ,
        NE,
        NullEQ,
    }

    #[allow(clippy::type_complexity)]
    fn generate_numeric_compare_cases(
    ) -> Vec<(Option<f64>, Option<f64>, TestCaseCmpOp, Option<i64>)> {
        vec![
            (None, None, TestCaseCmpOp::GT, None),
            (Some(3.5), None, TestCaseCmpOp::GT, None),
            (Some(-2.1), None, TestCaseCmpOp::GT, None),
            (None, Some(3.5), TestCaseCmpOp::GT, None),
            (None, Some(-2.1), TestCaseCmpOp::GT, None),
            (Some(3.5), Some(-2.1), TestCaseCmpOp::GT, Some(1)),
            (Some(-2.1), Some(3.5), TestCaseCmpOp::GT, Some(0)),
            (Some(3.5), Some(3.5), TestCaseCmpOp::GT, Some(0)),
            (Some(-2.1), Some(-2.1), TestCaseCmpOp::GT, Some(0)),
            (None, None, TestCaseCmpOp::GE, None),
            (Some(3.5), None, TestCaseCmpOp::GE, None),
            (Some(-2.1), None, TestCaseCmpOp::GE, None),
            (None, Some(3.5), TestCaseCmpOp::GE, None),
            (None, Some(-2.1), TestCaseCmpOp::GE, None),
            (Some(3.5), Some(-2.1), TestCaseCmpOp::GE, Some(1)),
            (Some(-2.1), Some(3.5), TestCaseCmpOp::GE, Some(0)),
            (Some(3.5), Some(3.5), TestCaseCmpOp::GE, Some(1)),
            (Some(-2.1), Some(-2.1), TestCaseCmpOp::GE, Some(1)),
            (None, None, TestCaseCmpOp::LT, None),
            (Some(3.5), None, TestCaseCmpOp::LT, None),
            (Some(-2.1), None, TestCaseCmpOp::LT, None),
            (None, Some(3.5), TestCaseCmpOp::LT, None),
            (None, Some(-2.1), TestCaseCmpOp::LT, None),
            (Some(3.5), Some(-2.1), TestCaseCmpOp::LT, Some(0)),
            (Some(-2.1), Some(3.5), TestCaseCmpOp::LT, Some(1)),
            (Some(3.5), Some(3.5), TestCaseCmpOp::LT, Some(0)),
            (Some(-2.1), Some(-2.1), TestCaseCmpOp::LT, Some(0)),
            (None, None, TestCaseCmpOp::LE, None),
            (Some(3.5), None, TestCaseCmpOp::LE, None),
            (Some(-2.1), None, TestCaseCmpOp::LE, None),
            (None, Some(3.5), TestCaseCmpOp::LE, None),
            (None, Some(-2.1), TestCaseCmpOp::LE, None),
            (Some(3.5), Some(-2.1), TestCaseCmpOp::LE, Some(0)),
            (Some(-2.1), Some(3.5), TestCaseCmpOp::LE, Some(1)),
            (Some(3.5), Some(3.5), TestCaseCmpOp::LE, Some(1)),
            (Some(-2.1), Some(-2.1), TestCaseCmpOp::LE, Some(1)),
            (None, None, TestCaseCmpOp::EQ, None),
            (Some(3.5), None, TestCaseCmpOp::EQ, None),
            (Some(-2.1), None, TestCaseCmpOp::EQ, None),
            (None, Some(3.5), TestCaseCmpOp::EQ, None),
            (None, Some(-2.1), TestCaseCmpOp::EQ, None),
            (Some(3.5), Some(-2.1), TestCaseCmpOp::EQ, Some(0)),
            (Some(-2.1), Some(3.5), TestCaseCmpOp::EQ, Some(0)),
            (Some(3.5), Some(3.5), TestCaseCmpOp::EQ, Some(1)),
            (Some(-2.1), Some(-2.1), TestCaseCmpOp::EQ, Some(1)),
            (None, None, TestCaseCmpOp::NE, None),
            (Some(3.5), None, TestCaseCmpOp::NE, None),
            (Some(-2.1), None, TestCaseCmpOp::NE, None),
            (None, Some(3.5), TestCaseCmpOp::NE, None),
            (None, Some(-2.1), TestCaseCmpOp::NE, None),
            (Some(3.5), Some(-2.1), TestCaseCmpOp::NE, Some(1)),
            (Some(-2.1), Some(3.5), TestCaseCmpOp::NE, Some(1)),
            (Some(3.5), Some(3.5), TestCaseCmpOp::NE, Some(0)),
            (Some(-2.1), Some(-2.1), TestCaseCmpOp::NE, Some(0)),
            (None, None, TestCaseCmpOp::NullEQ, Some(1)),
            (Some(3.5), None, TestCaseCmpOp::NullEQ, Some(0)),
            (Some(-2.1), None, TestCaseCmpOp::NullEQ, Some(0)),
            (None, Some(3.5), TestCaseCmpOp::NullEQ, Some(0)),
            (None, Some(-2.1), TestCaseCmpOp::NullEQ, Some(0)),
            (Some(3.5), Some(-2.1), TestCaseCmpOp::NullEQ, Some(0)),
            (Some(-2.1), Some(3.5), TestCaseCmpOp::NullEQ, Some(0)),
            (Some(3.5), Some(3.5), TestCaseCmpOp::NullEQ, Some(1)),
            (Some(-2.1), Some(-2.1), TestCaseCmpOp::NullEQ, Some(1)),
        ]
    }

    #[test]
    fn test_compare_real() {
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GTReal,
                TestCaseCmpOp::GE => ScalarFuncSig::GEReal,
                TestCaseCmpOp::LT => ScalarFuncSig::LTReal,
                TestCaseCmpOp::LE => ScalarFuncSig::LEReal,
                TestCaseCmpOp::EQ => ScalarFuncSig::EQReal,
                TestCaseCmpOp::NE => ScalarFuncSig::NEReal,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEQReal,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0)
                .push_param(arg1)
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_duration() {
        fn map_double_to_duration(v: f64) -> Duration {
            let d = std::time::Duration::from_millis((v.abs() * 1000.0) as u64);
            let is_neg = v < 0.0;
            Duration::new(d, is_neg, 4).unwrap()
        }

        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GTDuration,
                TestCaseCmpOp::GE => ScalarFuncSig::GEDuration,
                TestCaseCmpOp::LT => ScalarFuncSig::LTDuration,
                TestCaseCmpOp::LE => ScalarFuncSig::LEDuration,
                TestCaseCmpOp::EQ => ScalarFuncSig::EQDuration,
                TestCaseCmpOp::NE => ScalarFuncSig::NEDuration,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEQDuration,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(map_double_to_duration))
                .push_param(arg1.map(map_double_to_duration))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_decimal() {
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GTDecimal,
                TestCaseCmpOp::GE => ScalarFuncSig::GEDecimal,
                TestCaseCmpOp::LT => ScalarFuncSig::LTDecimal,
                TestCaseCmpOp::LE => ScalarFuncSig::LEDecimal,
                TestCaseCmpOp::EQ => ScalarFuncSig::EQDecimal,
                TestCaseCmpOp::NE => ScalarFuncSig::NEDecimal,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEQDecimal,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(|v| Decimal::from_f64(v).unwrap()))
                .push_param(arg1.map(|v| Decimal::from_f64(v).unwrap()))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_signed_int() {
        for (arg0, arg1, cmp_op, expect_output) in generate_numeric_compare_cases() {
            let sig = match cmp_op {
                TestCaseCmpOp::GT => ScalarFuncSig::GTInt,
                TestCaseCmpOp::GE => ScalarFuncSig::GEInt,
                TestCaseCmpOp::LT => ScalarFuncSig::LTInt,
                TestCaseCmpOp::LE => ScalarFuncSig::LEInt,
                TestCaseCmpOp::EQ => ScalarFuncSig::EQInt,
                TestCaseCmpOp::NE => ScalarFuncSig::NEInt,
                TestCaseCmpOp::NullEQ => ScalarFuncSig::NullEQInt,
            };
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg0.map(|v| v as i64))
                .push_param(arg1.map(|v| v as i64))
                .evaluate(sig)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}, {:?}, {:?}", arg0, arg1, sig);
        }
    }

    #[test]
    fn test_compare_int_2() {
        let test_cases = vec![
            (Some(5), false, Some(3), false, Ordering::Greater),
            (
                Some(std::u64::MAX as i64),
                false,
                Some(5),
                false,
                Ordering::Less,
            ),
            (
                Some(std::u64::MAX as i64),
                true,
                Some((std::u64::MAX - 1) as i64),
                true,
                Ordering::Greater,
            ),
            (
                Some(std::u64::MAX as i64),
                true,
                Some(5),
                true,
                Ordering::Greater,
            ),
            (Some(5), true, Some(std::i64::MIN), false, Ordering::Greater),
            (
                Some(std::u64::MAX as i64),
                true,
                Some(std::i64::MIN),
                false,
                Ordering::Greater,
            ),
            (Some(5), true, Some(3), false, Ordering::Greater),
            (Some(std::i64::MIN), false, Some(3), true, Ordering::Less),
            (
                Some(5),
                false,
                Some(std::u64::MAX as i64),
                true,
                Ordering::Less,
            ),
            (Some(5), false, Some(3), true, Ordering::Greater),
        ];
        for (lhs, lhs_is_unsigned, rhs, rhs_is_unsigned, ordering) in test_cases {
            let lhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if lhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();
            let rhs_field_type = FieldTypeBuilder::new()
                .tp(FieldTypeTp::LongLong)
                .flag(if rhs_is_unsigned {
                    FieldTypeFlag::UNSIGNED
                } else {
                    FieldTypeFlag::empty()
                })
                .build();

            for (sig, accept_orderings) in &[
                (ScalarFuncSig::EQInt, vec![Ordering::Equal]),
                (
                    ScalarFuncSig::NEInt,
                    vec![Ordering::Greater, Ordering::Less],
                ),
                (ScalarFuncSig::GTInt, vec![Ordering::Greater]),
                (
                    ScalarFuncSig::GEInt,
                    vec![Ordering::Greater, Ordering::Equal],
                ),
                (ScalarFuncSig::LTInt, vec![Ordering::Less]),
                (ScalarFuncSig::LEInt, vec![Ordering::Less, Ordering::Equal]),
            ] {
                let output = RpnFnScalarEvaluator::new()
                    .push_param_with_field_type(lhs, lhs_field_type.clone())
                    .push_param_with_field_type(rhs, rhs_field_type.clone())
                    .evaluate(sig.clone())
                    .unwrap();
                if accept_orderings.iter().any(|&x| x == ordering) {
                    assert_eq!(output, Some(1));
                } else {
                    assert_eq!(output, Some(0));
                }
            }
        }
    }
}
