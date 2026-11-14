//! Text exposition writer for the Prometheus 0.0.4 format.

/// Prometheus metric type announced in the `# TYPE` line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
	/// Monotonic counter.
	Counter,
	/// Value that can go up and down.
	Gauge,
	/// Bucketed distribution.
	Histogram,
}

/// Decimal scale between the integer a metric stores and the value exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scale {
	/// Exposed as stored.
	One,
	/// Stored in millionths, exposed in whole units.
	Micro,
	/// Stored in billionths, exposed in whole units.
	Nano,
}

/// Sample value, formatted without going through floating point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Value {
	/// Unsigned integer.
	Unsigned(u64),
	/// Signed integer.
	Signed(i64),
	/// Unsigned integer divided by a decimal scale.
	Scaled(u64, Scale),
}

/// Upper bound label of a histogram bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Le {
	/// Finite bound in stored units.
	Bound(u64, Scale),
	/// The `+Inf` bucket.
	Inf,
}

/// Accumulates the exposition text of one scrape.
///
/// Formatting is infallible: output goes to a `String` and numbers are
/// rendered by hand.
#[derive(Debug, Default)]
pub struct Writer {
	out: String,
	overflow: Vec<(String, u64)>,
}

const OVERFLOW_NAME: &str = "tuwunel_metrics_series_overflow_total";
const OVERFLOW_HELP: &str =
	"Observations diverted to the _other series because a family reached its series cap.";

impl Kind {
	const fn name(self) -> &'static str {
		match self {
			| Self::Counter => "counter",
			| Self::Gauge => "gauge",
			| Self::Histogram => "histogram",
		}
	}
}

impl Writer {
	#[must_use]
	/// Creates an empty writer.
	pub fn new() -> Self {
		Self {
			out: String::with_capacity(64 * 1024),
			overflow: Vec::new(),
		}
	}

	/// Writes the `# HELP` and `# TYPE` lines that open a family.
	pub fn family(&mut self, name: &str, help: &str, kind: Kind) {
		self.out.push_str("# HELP ");
		self.out.push_str(name);
		self.out.push(' ');
		push_escaped(&mut self.out, help, false);
		self.out.push('\n');
		self.out.push_str("# TYPE ");
		self.out.push_str(name);
		self.out.push(' ');
		self.out.push_str(kind.name());
		self.out.push('\n');
	}

	/// Writes one sample line.
	///
	/// `labels` is a rendered label body without braces, built with
	/// [`push_label`]. `suffix` is appended to the metric name, as in
	/// `_bucket`, `_sum` and `_count`.
	pub fn sample(
		&mut self,
		name: &str,
		suffix: &str,
		labels: &str,
		le: Option<Le>,
		value: Value,
	) {
		self.out.push_str(name);
		self.out.push_str(suffix);

		if !labels.is_empty() || le.is_some() {
			self.out.push('{');
			self.out.push_str(labels);
			if let Some(le) = le {
				if !labels.is_empty() {
					self.out.push(',');
				}

				self.out.push_str("le=\"");
				match le {
					| Le::Bound(bound, scale) => push_scaled(&mut self.out, bound, scale),
					| Le::Inf => self.out.push_str("+Inf"),
				}
				self.out.push('"');
			}
			self.out.push('}');
		}

		self.out.push(' ');
		match value {
			| Value::Unsigned(value) => push_u64(&mut self.out, value),
			| Value::Signed(value) => push_i64(&mut self.out, value),
			| Value::Scaled(value, scale) => push_scaled(&mut self.out, value, scale),
		}
		self.out.push('\n');
	}

	/// Writes a whole family: its header followed by every series of `metric`.
	pub fn metric<M>(&mut self, name: &str, help: &str, metric: &M)
	where
		M: super::Metric,
	{
		self.family(name, help, M::KIND);
		metric.write(self, name, "");
	}

	/// Records that `count` observations of the family `name` were diverted to
	/// its `_other` series. Emitted by [`Self::finish`].
	pub fn overflow(&mut self, name: &str, count: u64) {
		self.overflow.push((name.to_owned(), count));
	}

	#[must_use]
	/// Completes the scrape and returns the exposition text.
	pub fn finish(mut self) -> String {
		let mut overflow = std::mem::take(&mut self.overflow);
		overflow.sort();

		self.family(OVERFLOW_NAME, OVERFLOW_HELP, Kind::Counter);
		let mut labels = String::new();
		for (name, count) in overflow {
			labels.clear();
			push_label(&mut labels, "family", &name);
			self.sample(OVERFLOW_NAME, "", &labels, None, Value::Unsigned(count));
		}

		self.out
	}
}

/// Appends `name="value"` to a label body, adding a comma when the body is not
/// empty. Backslash, double quote and newline in the value are escaped.
pub fn push_label(out: &mut String, name: &str, value: &str) {
	if !out.is_empty() {
		out.push(',');
	}

	out.push_str(name);
	out.push_str("=\"");
	push_escaped(out, value, true);
	out.push('"');
}

fn push_escaped(out: &mut String, text: &str, quote: bool) {
	for ch in text.chars() {
		match ch {
			| '\\' => out.push_str("\\\\"),
			| '\n' => out.push_str("\\n"),
			| '"' if quote => out.push_str("\\\""),
			| ch => out.push(ch),
		}
	}
}

fn push_u64(out: &mut String, mut value: u64) {
	let mut digits = [b'0'; 20];
	let mut start = digits.len();
	loop {
		start = start.saturating_sub(1);
		let digit = u32::try_from(value % 10).unwrap_or(0);
		digits[start] = char::from_digit(digit, 10)
			.and_then(|ch| u8::try_from(ch).ok())
			.unwrap_or(b'0');
		value /= 10;
		if value == 0 || start == 0 {
			break;
		}
	}

	out.extend(digits[start..].iter().copied().map(char::from));
}

fn push_i64(out: &mut String, value: i64) {
	if value < 0 {
		out.push('-');
	}

	push_u64(out, value.unsigned_abs());
}

fn push_scaled(out: &mut String, value: u64, scale: Scale) {
	match scale {
		| Scale::One => push_u64(out, value),
		| Scale::Micro => push_fraction(out, value / 1_000_000, value % 1_000_000, 6),
		| Scale::Nano => push_fraction(out, value / 1_000_000_000, value % 1_000_000_000, 9),
	}
}

fn push_fraction(out: &mut String, whole: u64, mut fraction: u64, width: usize) {
	push_u64(out, whole);
	if fraction == 0 {
		return;
	}

	let mut digits = [b'0'; 9];
	let mut index = width.min(digits.len());
	let width = index;
	while index > 0 {
		index = index.saturating_sub(1);
		let digit = u32::try_from(fraction % 10).unwrap_or(0);
		digits[index] = char::from_digit(digit, 10)
			.and_then(|ch| u8::try_from(ch).ok())
			.unwrap_or(b'0');
		fraction /= 10;
	}

	let mut end = width;
	while end > 0 && digits[end.saturating_sub(1)] == b'0' {
		end = end.saturating_sub(1);
	}

	out.push('.');
	out.extend(digits[..end].iter().copied().map(char::from));
}
