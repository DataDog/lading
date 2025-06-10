//! Code to generate units according to
//! <http://unitsofmeasure.org/ucum.html>. This may be generally
//! useful in the project and is not specifically tied to the OpenTelemetry metrics
//! implementation.

// The spec defines a fairly elaborate grammar. It's unclear that we need _all_
// that for our purposes, so for now we rely on a simple table method. I imagine
// we can just keep stuffing the table for a while as seems desirable.

use super::templates::GeneratorError;

const UNITS: &[&str] = &[
    "bit", "Kbit", "Mbit", "Gbit", // data size, bits, decimal
    "Kibit", "Mibit", "Gibit", // data size, bits, binary
    "By", "KBy", "MBy", "GBy", // data size, bytes, decimal
    "KiBy", "MiBy", "GiBy", // data size, bytes, binary
    "By/s", "KiBy/s", "bit/s", "Mbit/s", // transfer rate
    "s", "ms", "us", "ns", // time
    "W", "kW", "kWh", // power
];

/// Errors related to generation
#[derive(thiserror::Error, Debug, Clone, Copy)]
pub enum Error {}

#[derive(Debug, Clone, Copy)]
pub(crate) struct UnitGenerator {}

impl UnitGenerator {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl crate::Generator<'_> for UnitGenerator {
    type Output = &'static str;
    type Error = GeneratorError;

    fn generate<R>(&self, rng: &mut R) -> Result<Self::Output, Self::Error>
    where
        R: rand::Rng + ?Sized,
    {
        Ok(UNITS[rng.random_range(0..UNITS.len())])
    }
}
