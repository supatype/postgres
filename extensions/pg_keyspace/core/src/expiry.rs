//! Expiry arguments, with Valkey's accept and refuse boundaries.
//!
//! Valkey works in int64 milliseconds and refuses an expiry whose arithmetic
//! would overflow: converting seconds to milliseconds, or adding the base time
//! of a relative form. Saturating or wrapping instead is how a deadline of
//! nonsense got stored here, so every check below is the overflow check Valkey
//! makes, in the order it makes it, rather than a range invented for this crate.
//!
//! The store keeps deadlines as int64 MICROseconds, which tops out at
//! 9223372036854775 ms (the year 294247). Valkey accepts deadlines up to
//! 2^63-1 ms. A deadline past what the store can hold is accepted exactly as
//! Valkey accepts it and clamped by [`deadline_micros`]; it is the one place
//! the reported TTL can differ from Valkey's, and only beyond that year.


/// The unit an expiry argument is given in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Seconds,
    Millis,
}

impl Unit {
    fn to_millis(self, n: i64) -> Option<i64> {
        match self {
            Unit::Seconds => n.checked_mul(1000),
            Unit::Millis => Some(n),
        }
    }
}

/// Which of Valkey's two sets of boundaries an expiry argument is held to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT. Zero and negative values are
    /// accepted: the deadline is then in the past and the key is deleted.
    Expire,
    /// SET EX/PX/EXAT/PXAT, GETEX, SETEX and PSETEX. The value itself must be
    /// positive, so `SET k v EXAT 0` is refused while `EXPIREAT k 0` deletes `k`.
    Set,
}

/// Why an expiry argument was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    NotInt,
    Invalid,
}

impl Refusal {
    /// The error line Valkey sends for this refusal; `command` is lower case.
    pub fn message(self, command: &str) -> String {
        match self {
            Refusal::NotInt => "ERR value is not an integer or out of range".into(),
            Refusal::Invalid => format!("ERR invalid expire time in '{command}' command"),
        }
    }
}

/// The deadline, in unix milliseconds, for `n` in `unit` added to `base_ms`
/// (now for a relative form, 0 for an absolute one). Refused when `rule`
/// refuses the value, or converting seconds to milliseconds overflows, or
/// adding the base does: Valkey's checks, in Valkey's order.
fn deadline_ms(n: i64, unit: Unit, base_ms: i64, rule: Rule) -> Option<i64> {
    if rule == Rule::Set && n <= 0 {
        return None;
    }
    unit.to_millis(n)?.checked_add(base_ms)
}

/// The store's deadline (unix micros) for a deadline in unix milliseconds.
///
/// Never 0, which the store reads as "no expiry": a deadline at or before the
/// epoch becomes 1 us, which is already past, so the key is deleted. A deadline
/// beyond the store's range saturates at its latest representable instant.
pub fn deadline_micros(deadline_ms: i64) -> i64 {
    deadline_ms.saturating_mul(1000).max(1)
}

/// The current time in unix milliseconds, from the store's clock.
fn now_ms() -> i64 {
    crate::store::now_micros() / 1000
}

/// The four ways to give an expiry: relative or absolute, seconds or millis.
/// SET and GETEX name them as options; EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT,
/// SETEX and PSETEX each imply one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpireOpt {
    Ex,
    Px,
    ExAt,
    PxAt,
}

impl ExpireOpt {
    fn from_name(name: &[u8]) -> Option<Self> {
        match name {
            b"EX" => Some(ExpireOpt::Ex),
            b"PX" => Some(ExpireOpt::Px),
            b"EXAT" => Some(ExpireOpt::ExAt),
            b"PXAT" => Some(ExpireOpt::PxAt),
            _ => None,
        }
    }

    /// The deadline in unix milliseconds for the argument `arg`, held to `rule`.
    pub fn deadline_ms(self, arg: &[u8], rule: Rule) -> Result<i64, Refusal> {
        let n = crate::resp::arg_int(arg).ok_or(Refusal::NotInt)?;
        let (unit, base) = match self {
            ExpireOpt::Ex => (Unit::Seconds, now_ms()),
            ExpireOpt::Px => (Unit::Millis, now_ms()),
            ExpireOpt::ExAt => (Unit::Seconds, 0),
            ExpireOpt::PxAt => (Unit::Millis, 0),
        };
        deadline_ms(n, unit, base, rule).ok_or(Refusal::Invalid)
    }
}

/// Which command's options are being parsed: they share one grammar, but NX,
/// XX, GET and KEEPTTL belong to SET and PERSIST to GETEX.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringCmd {
    Set,
    GetEx,
}

/// The parsed options of SET or GETEX. The expiry argument is kept unparsed:
/// Valkey reports a syntax error anywhere in the options before it looks at
/// the number, so `SET k v EX abc PX 1` is a syntax error, not "not an integer".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StringOpts<'a> {
    pub nx: bool,
    pub xx: bool,
    pub get: bool,
    pub keep_ttl: bool,
    pub persist: bool,
    pub expire: Option<(ExpireOpt, &'a [u8])>,
}

/// Parse the options of SET (`opts` = everything after the value) or GETEX
/// (everything after the key). `None` is "ERR syntax error".
///
/// The conflict rules are Valkey's `parseExtendedStringArgumentsOrReply`:
/// EX/PX/EXAT/PXAT, KEEPTTL and PERSIST exclude each other, NX excludes XX, and
/// repeating the same option is allowed, the last value winning.
pub fn parse_string_opts(opts: &[Vec<u8>], cmd: StringCmd) -> Option<StringOpts<'_>> {
    let set = cmd == StringCmd::Set;
    let mut o = StringOpts::default();
    let mut i = 0;
    while i < opts.len() {
        let name = opts[i].to_ascii_uppercase();
        match name.as_slice() {
            b"NX" if set && !o.xx => o.nx = true,
            b"XX" if set && !o.nx => o.xx = true,
            b"GET" if set => o.get = true,
            b"KEEPTTL" if set && o.expire.is_none() => o.keep_ttl = true,
            b"PERSIST" if !set && o.expire.is_none() => o.persist = true,
            _ => {
                let e = ExpireOpt::from_name(&name)?;
                let arg = opts.get(i + 1)?;
                let other_expiry = o.expire.is_some_and(|(prev, _)| prev != e);
                if o.keep_ttl || o.persist || other_expiry {
                    return None;
                }
                o.expire = Some((e, arg.as_slice()));
                i += 1;
            }
        }
        i += 1;
    }
    Some(o)
}

/// The NX / XX / GT / LT conditions of the EXPIRE family.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExpireCond {
    pub nx: bool,
    pub xx: bool,
    pub gt: bool,
    pub lt: bool,
}

impl ExpireCond {
    /// Parse the options after `EXPIRE key value`, or the exact error line
    /// Valkey sends for them.
    pub fn parse(opts: &[Vec<u8>]) -> Result<Self, String> {
        let mut c = ExpireCond::default();
        for opt in opts {
            let flag = match opt.as_slice() {
                o if o.eq_ignore_ascii_case(b"NX") => &mut c.nx,
                o if o.eq_ignore_ascii_case(b"XX") => &mut c.xx,
                o if o.eq_ignore_ascii_case(b"GT") => &mut c.gt,
                o if o.eq_ignore_ascii_case(b"LT") => &mut c.lt,
                _ => {
                    let opt = String::from_utf8_lossy(opt);
                    return Err(format!("ERR Unsupported option {opt}"));
                }
            };
            *flag = true;
        }
        if c.nx && (c.xx || c.gt || c.lt) {
            return Err(
                "ERR NX and XX, GT or LT options at the same time are not compatible".into(),
            );
        }
        if c.gt && c.lt {
            return Err("ERR GT and LT options at the same time are not compatible".into());
        }
        Ok(c)
    }

    /// Whether the new deadline applies, given the key's current one
    /// (`None` = no expiry). Both in unix milliseconds. A key with no expiry
    /// counts as expiring never: GT never beats it and LT always does.
    pub fn allows(self, new_ms: i64, current_ms: Option<i64>) -> bool {
        match current_ms {
            None => !(self.xx || self.gt),
            Some(cur) => !(self.nx || (self.gt && new_ms <= cur) || (self.lt && new_ms >= cur)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_S: i64 = i64::MAX / 1000; // 9223372036854775

    fn expire_deadline_ms(n: i64, unit: Unit, base_ms: i64) -> Option<i64> {
        deadline_ms(n, unit, base_ms, Rule::Expire)
    }

    fn set_deadline_ms(n: i64, unit: Unit, base_ms: i64) -> Option<i64> {
        deadline_ms(n, unit, base_ms, Rule::Set)
    }

    #[test]
    fn expire_refuses_only_overflow() {
        let now = 1_792_000_000_000;
        // Seconds -> millis overflows at exactly i64::MAX / 1000 + 1, both signs.
        assert_eq!(expire_deadline_ms(MAX_S, Unit::Seconds, 0), Some(MAX_S * 1000));
        assert_eq!(expire_deadline_ms(MAX_S + 1, Unit::Seconds, 0), None);
        assert_eq!(expire_deadline_ms(-MAX_S, Unit::Seconds, 0), Some(-MAX_S * 1000));
        assert_eq!(expire_deadline_ms(-MAX_S - 1, Unit::Seconds, 0), None);
        assert_eq!(expire_deadline_ms(-(1 << 62), Unit::Seconds, 0), None);
        assert_eq!(expire_deadline_ms(99_999_999_999_999_999, Unit::Seconds, 0), None);
        // The base addition overflows for a relative form only.
        assert_eq!(expire_deadline_ms(MAX_S, Unit::Seconds, now), None);
        assert_eq!(expire_deadline_ms(i64::MAX, Unit::Millis, 0), Some(i64::MAX));
        assert_eq!(expire_deadline_ms(i64::MAX - now + 1, Unit::Millis, now), None);
        assert_eq!(expire_deadline_ms(i64::MAX - now, Unit::Millis, now), Some(i64::MAX));
        // Past and negative deadlines are accepted; they delete the key.
        assert_eq!(expire_deadline_ms(0, Unit::Seconds, 0), Some(0));
        assert_eq!(expire_deadline_ms(i64::MIN, Unit::Millis, now), Some(i64::MIN + now));
    }

    #[test]
    fn set_family_also_refuses_non_positive() {
        let now = 1_792_000_000_000;
        assert_eq!(set_deadline_ms(0, Unit::Seconds, 0), None);
        assert_eq!(set_deadline_ms(-1, Unit::Millis, now), None);
        assert_eq!(set_deadline_ms(1, Unit::Seconds, 0), Some(1000));
        assert_eq!(set_deadline_ms(MAX_S, Unit::Seconds, 0), Some(MAX_S * 1000));
        assert_eq!(set_deadline_ms(MAX_S, Unit::Seconds, now), None);
        assert_eq!(set_deadline_ms(MAX_S + 1, Unit::Seconds, 0), None);
        assert_eq!(set_deadline_ms(i64::MAX, Unit::Millis, 0), Some(i64::MAX));
        assert_eq!(set_deadline_ms(i64::MAX, Unit::Millis, now), None);
    }

    #[test]
    fn parse_classifies_refusals() {
        let r = |a: &[u8]| ExpireOpt::ExAt.deadline_ms(a, Rule::Expire);
        assert_eq!(r(b"abc"), Err(Refusal::NotInt));
        assert_eq!(r(b"9223372036854775808"), Err(Refusal::NotInt));
        assert_eq!(r(b"99999999999999999"), Err(Refusal::Invalid));
        assert_eq!(r(b"10"), Ok(10_000));
        assert_eq!(r(b"0"), Ok(0));
        assert_eq!(ExpireOpt::ExAt.deadline_ms(b"0", Rule::Set), Err(Refusal::Invalid));
        let px = ExpireOpt::Px.deadline_ms(b"60000", Rule::Set).unwrap() - now_ms();
        assert!((59_000..=60_000).contains(&px), "{px}");
        assert_eq!(
            Refusal::Invalid.message("psetex"),
            "ERR invalid expire time in 'psetex' command"
        );
        assert_eq!(Refusal::NotInt.message("set"), "ERR value is not an integer or out of range");
    }

    #[test]
    fn stored_deadline_is_never_no_expiry() {
        assert_eq!(deadline_micros(0), 1);
        assert_eq!(deadline_micros(-5), 1);
        assert_eq!(deadline_micros(i64::MIN), 1);
        assert_eq!(deadline_micros(1500), 1_500_000);
        assert_eq!(deadline_micros(MAX_S), MAX_S * 1000);
        assert_eq!(deadline_micros(MAX_S + 1), i64::MAX);
        assert_eq!(deadline_micros(i64::MAX), i64::MAX);
    }

    fn args(s: &str) -> Vec<Vec<u8>> {
        s.split_whitespace().map(|w| w.as_bytes().to_vec()).collect()
    }

    #[test]
    fn string_opts_follow_valkeys_conflict_rules() {
        type Seen = Option<(Option<(ExpireOpt, Vec<u8>)>, bool)>;
        let parse = |s: &str, cmd: StringCmd| -> Seen {
            let a = args(s);
            let o = parse_string_opts(&a, cmd)?;
            let flag = if cmd == StringCmd::Set { o.keep_ttl } else { o.persist };
            Some((o.expire.map(|(e, n)| (e, n.to_vec())), flag))
        };
        let set = |s: &str| parse(s, StringCmd::Set);
        let getex = |s: &str| parse(s, StringCmd::GetEx);
        assert_eq!(set("EX 10 EX 20"), Some((Some((ExpireOpt::Ex, b"20".to_vec())), false)));
        assert_eq!(set("ex abc"), Some((Some((ExpireOpt::Ex, b"abc".to_vec())), false)));
        assert_eq!(set("KEEPTTL KEEPTTL"), Some((None, true)));
        for bad in ["EX", "EX 10 PX 10", "KEEPTTL EX 10", "EX 10 KEEPTTL", "EXAT 1 PXAT 1",
                    "EX abc PX 10", "NX XX", "XX NX", "PERSIST", "FOO"] {
            assert_eq!(set(bad), None, "SET {bad}");
        }
        assert!(parse_string_opts(&args("NX NX GET GET"), StringCmd::Set).is_some());
        assert_eq!(getex("PERSIST PERSIST"), Some((None, true)));
        assert_eq!(getex(""), Some((None, false)));
        for bad in ["EX", "FOO", "KEEPTTL", "NX", "GET", "PERSIST EX 10", "EX 10 PERSIST",
                    "EX 10 PX 10"] {
            assert_eq!(getex(bad), None, "GETEX {bad}");
        }
    }

    #[test]
    fn expire_conditions() {
        let p = |s: &str| ExpireCond::parse(&args(s));
        assert!(p("nx").is_ok_and(|c| c.nx));
        assert!(p("XX GT").is_ok());
        assert!(p("XX LT").is_ok());
        assert_eq!(p("FOO"), Err("ERR Unsupported option FOO".into()));
        let nx_err = Err("ERR NX and XX, GT or LT options at the same time are not compatible".into());
        assert_eq!(p("NX XX"), nx_err);
        assert_eq!(p("GT NX"), nx_err);
        assert_eq!(p("GT LT"), Err("ERR GT and LT options at the same time are not compatible".into()));

        let c = |s: &str| p(s).unwrap();
        assert!(c("NX").allows(5, None) && !c("NX").allows(5, Some(9)));
        assert!(!c("XX").allows(5, None) && c("XX").allows(5, Some(9)));
        assert!(!c("GT").allows(5, None) && !c("GT").allows(9, Some(9)) && c("GT").allows(10, Some(9)));
        assert!(c("LT").allows(5, None) && !c("LT").allows(9, Some(9)) && c("LT").allows(8, Some(9)));
        assert!(c("").allows(5, None) && c("").allows(5, Some(9)));
    }
}
