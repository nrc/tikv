// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
pub mod function;
pub mod types;

pub mod impl_arithmetic;
pub mod impl_cast;
pub mod impl_compare;
pub mod impl_control;
pub mod impl_like;
pub mod impl_op;

pub use self::function::RpnFnMeta;
pub use self::types::{RpnExpression, RpnExpressionBuilder};

use cop_datatype::{FieldTypeAccessor, FieldTypeFlag};
use tipb::{Expr, ScalarFuncSig};

use self::impl_arithmetic::*;
use self::impl_compare::*;
use self::impl_control::*;
use self::impl_like::*;
use self::impl_op::*;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::Result;

fn map_int_sig<F>(value: ScalarFuncSig, children: &[Expr], mapper: F) -> Result<RpnFnMeta>
where
    F: Fn(bool, bool) -> RpnFnMeta,
{
    // FIXME: The signature for different signed / unsigned int should be inferred at TiDB side.
    if children.len() != 2 {
        return Err(box_err!(
            "ScalarFunction {:?} (params = {}) is not supported in batch mode",
            value,
            children.len()
        ));
    }
    let lhs_is_unsigned =
        FieldTypeAccessor::flag(children[0].get_field_type()).contains(FieldTypeFlag::UNSIGNED);
    let rhs_is_unsigned =
        FieldTypeAccessor::flag(children[1].get_field_type()).contains(FieldTypeFlag::UNSIGNED);
    Ok(mapper(lhs_is_unsigned, rhs_is_unsigned))
}

fn compare_mapper<F: CmpOp>(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => compare_fn_meta::<BasicComparer<Int, F>>(),
        (false, true) => compare_fn_meta::<IntUintComparer<F>>(),
        (true, false) => compare_fn_meta::<UintIntComparer<F>>(),
        (true, true) => compare_fn_meta::<UintUintComparer<F>>(),
    }
}

fn plus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntIntPlus>(),
        (false, true) => arithmetic_fn_meta::<IntUintPlus>(),
        (true, false) => arithmetic_fn_meta::<UintIntPlus>(),
        (true, true) => arithmetic_fn_meta::<UintUintPlus>(),
    }
}

fn minus_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntIntMinus>(),
        (false, true) => arithmetic_fn_meta::<IntUintMinus>(),
        (true, false) => arithmetic_fn_meta::<UintIntMinus>(),
        (true, true) => arithmetic_fn_meta::<UintUintMinus>(),
    }
}

fn mod_mapper(lhs_is_unsigned: bool, rhs_is_unsigned: bool) -> RpnFnMeta {
    match (lhs_is_unsigned, rhs_is_unsigned) {
        (false, false) => arithmetic_fn_meta::<IntIntMod>(),
        (false, true) => arithmetic_fn_meta::<IntUintMod>(),
        (true, false) => arithmetic_fn_meta::<UintIntMod>(),
        (true, true) => arithmetic_fn_meta::<UintUintMod>(),
    }
}

#[rustfmt::skip]
fn map_pb_sig_to_rpn_func(value: ScalarFuncSig, children: &[Expr]) -> Result<RpnFnMeta> {
    Ok(match value {
        ScalarFuncSig::LtInt => map_int_sig(value, children, compare_mapper::<CmpOpLT>)?,
        ScalarFuncSig::LtReal => compare_fn_meta::<BasicComparer<Real, CmpOpLT>>(),
        ScalarFuncSig::LtDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpLT>>(),
        ScalarFuncSig::LtString => compare_fn_meta::<BasicComparer<Bytes, CmpOpLT>>(),
        ScalarFuncSig::LtTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpLT>>(),
        ScalarFuncSig::LtDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpLT>>(),
        ScalarFuncSig::LtJson => compare_fn_meta::<BasicComparer<Json, CmpOpLT>>(),
        ScalarFuncSig::LeInt => map_int_sig(value, children, compare_mapper::<CmpOpLE>)?,
        ScalarFuncSig::LeReal => compare_fn_meta::<BasicComparer<Real, CmpOpLE>>(),
        ScalarFuncSig::LeDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpLE>>(),
        ScalarFuncSig::LeString => compare_fn_meta::<BasicComparer<Bytes, CmpOpLE>>(),
        ScalarFuncSig::LeTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpLE>>(),
        ScalarFuncSig::LeDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpLE>>(),
        ScalarFuncSig::LeJson => compare_fn_meta::<BasicComparer<Json, CmpOpLE>>(),
        ScalarFuncSig::GtInt => map_int_sig(value, children, compare_mapper::<CmpOpGT>)?,
        ScalarFuncSig::GtReal => compare_fn_meta::<BasicComparer<Real, CmpOpGT>>(),
        ScalarFuncSig::GtDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpGT>>(),
        ScalarFuncSig::GtString => compare_fn_meta::<BasicComparer<Bytes, CmpOpGT>>(),
        ScalarFuncSig::GtTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpGT>>(),
        ScalarFuncSig::GtDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpGT>>(),
        ScalarFuncSig::GtJson => compare_fn_meta::<BasicComparer<Json, CmpOpGT>>(),
        ScalarFuncSig::GeInt => map_int_sig(value, children, compare_mapper::<CmpOpGE>)?,
        ScalarFuncSig::GeReal => compare_fn_meta::<BasicComparer<Real, CmpOpGE>>(),
        ScalarFuncSig::GeDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpGE>>(),
        ScalarFuncSig::GeString => compare_fn_meta::<BasicComparer<Bytes, CmpOpGE>>(),
        ScalarFuncSig::GeTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpGE>>(),
        ScalarFuncSig::GeDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpGE>>(),
        ScalarFuncSig::GeJson => compare_fn_meta::<BasicComparer<Json, CmpOpGE>>(),
        ScalarFuncSig::NeInt => map_int_sig(value, children, compare_mapper::<CmpOpNE>)?,
        ScalarFuncSig::NeReal => compare_fn_meta::<BasicComparer<Real, CmpOpNE>>(),
        ScalarFuncSig::NeDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpNE>>(),
        ScalarFuncSig::NeString => compare_fn_meta::<BasicComparer<Bytes, CmpOpNE>>(),
        ScalarFuncSig::NeTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpNE>>(),
        ScalarFuncSig::NeDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpNE>>(),
        ScalarFuncSig::NeJson => compare_fn_meta::<BasicComparer<Json, CmpOpNE>>(),
        ScalarFuncSig::EqInt => map_int_sig(value, children, compare_mapper::<CmpOpEQ>)?,
        ScalarFuncSig::EqReal => compare_fn_meta::<BasicComparer<Real, CmpOpEQ>>(),
        ScalarFuncSig::EqDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpEQ>>(),
        ScalarFuncSig::EqString => compare_fn_meta::<BasicComparer<Bytes, CmpOpEQ>>(),
        ScalarFuncSig::EqTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpEQ>>(),
        ScalarFuncSig::EqDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpEQ>>(),
        ScalarFuncSig::EqJson => compare_fn_meta::<BasicComparer<Json, CmpOpEQ>>(),
        ScalarFuncSig::NullEqInt => map_int_sig(value, children, compare_mapper::<CmpOpNullEQ>)?,
        ScalarFuncSig::NullEqReal => compare_fn_meta::<BasicComparer<Real, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqDecimal => compare_fn_meta::<BasicComparer<Decimal, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqString => compare_fn_meta::<BasicComparer<Bytes, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqTime => compare_fn_meta::<BasicComparer<DateTime, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqDuration => compare_fn_meta::<BasicComparer<Duration, CmpOpNullEQ>>(),
        ScalarFuncSig::NullEqJson => compare_fn_meta::<BasicComparer<Json, CmpOpNullEQ>>(),
        ScalarFuncSig::IntIsNull => is_null_fn_meta::<Int>(),
        ScalarFuncSig::RealIsNull => is_null_fn_meta::<Real>(),
        ScalarFuncSig::DecimalIsNull => is_null_fn_meta::<Decimal>(),
        ScalarFuncSig::StringIsNull => is_null_fn_meta::<Bytes>(),
        ScalarFuncSig::TimeIsNull => is_null_fn_meta::<DateTime>(),
        ScalarFuncSig::DurationIsNull => is_null_fn_meta::<Duration>(),
        ScalarFuncSig::JsonIsNull => is_null_fn_meta::<Json>(),
        ScalarFuncSig::IntIsTrue => int_is_true_fn_meta(),
        ScalarFuncSig::RealIsTrue => real_is_true_fn_meta(),
        ScalarFuncSig::DecimalIsTrue => decimal_is_true_fn_meta(),
        ScalarFuncSig::IntIsFalse => int_is_false_fn_meta(),
        ScalarFuncSig::RealIsFalse => real_is_false_fn_meta(),
        ScalarFuncSig::DecimalIsFalse => decimal_is_false_fn_meta(),
        ScalarFuncSig::LogicalAnd => logical_and_fn_meta(),
        ScalarFuncSig::LogicalOr => logical_or_fn_meta(),
        ScalarFuncSig::UnaryNot => unary_not_fn_meta(),
        ScalarFuncSig::PlusInt => map_int_sig(value, children, plus_mapper)?,
        ScalarFuncSig::PlusReal => arithmetic_fn_meta::<RealPlus>(),
        ScalarFuncSig::PlusDecimal => arithmetic_fn_meta::<DecimalPlus>(),
        ScalarFuncSig::MinusInt => map_int_sig(value, children, minus_mapper)?,
        ScalarFuncSig::MinusReal => arithmetic_fn_meta::<RealMinus>(),
        ScalarFuncSig::MinusDecimal => arithmetic_fn_meta::<DecimalMinus>(),
        ScalarFuncSig::MultiplyDecimal => arithmetic_fn_meta::<DecimalMultiply>(),
        ScalarFuncSig::ModReal => arithmetic_fn_meta::<RealMod>(),
        ScalarFuncSig::ModDecimal => arithmetic_fn_meta::<DecimalMod>(),
        ScalarFuncSig::ModInt => map_int_sig(value, children, mod_mapper)?,
        ScalarFuncSig::LikeSig => like_fn_meta(),
        ScalarFuncSig::IfNullInt => if_null_fn_meta::<Int>(),
        ScalarFuncSig::IfNullReal => if_null_fn_meta::<Real>(),
        ScalarFuncSig::IfNullString => if_null_fn_meta::<Bytes>(),
        ScalarFuncSig::IfNullDecimal => if_null_fn_meta::<Decimal>(),
        ScalarFuncSig::IfNullTime => if_null_fn_meta::<DateTime>(),
        ScalarFuncSig::IfNullDuration => if_null_fn_meta::<Duration>(),
        ScalarFuncSig::IfNullJson => if_null_fn_meta::<Json>(),
        _ => return Err(box_err!(
            "ScalarFunction {:?} is not supported in batch mode",
            value
        )),
    })
}
