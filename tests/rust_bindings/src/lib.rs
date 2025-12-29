use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use nao_dedup::{deduplicate_read_pairs, DedupParams, MinimizerParams, ReadPair};

/// Helper to extract a field from a Python ReadPair object
fn extract_string(obj: &Bound<'_, PyAny>, field: &str) -> PyResult<String> {
    obj.getattr(field)?.extract()
}

/// Convert Python ReadPair to Rust ReadPair
fn py_to_rust_read_pair(py_rp: &Bound<'_, PyAny>) -> PyResult<ReadPair> {
    let read_id = extract_string(py_rp, "read_id")?;
    let fwd_seq = extract_string(py_rp, "fwd_seq")?;
    let rev_seq = extract_string(py_rp, "rev_seq")?;

    // Python ReadPair stores mean_q but not individual quality strings
    // Generate dummy quality strings that match the mean quality
    let mean_q: f64 = py_rp.getattr("mean_q")?.extract()?;
    let qual_char = ((mean_q.round() as u32) + 33) as u8 as char;
    let fwd_qual = qual_char.to_string().repeat(fwd_seq.len());
    let rev_qual = qual_char.to_string().repeat(rev_seq.len());

    Ok(ReadPair {
        read_id,
        fwd_seq,
        rev_seq,
        fwd_qual,
        rev_qual,
    })
}

/// Deduplicate read pairs using Rust implementation
#[pyfunction]
#[pyo3(signature = (read_pairs, dedup_params=None, minimizer_params=None, verbose=false))]
fn deduplicate_read_pairs_rust(
    py: Python<'_>,
    read_pairs: &Bound<'_, PyList>,
    dedup_params: Option<&Bound<'_, PyAny>>,
    minimizer_params: Option<&Bound<'_, PyAny>>,
    verbose: bool,
) -> PyResult<Py<PyDict>> {
    // Convert Python ReadPairs to Rust ReadPairs
    let mut rust_read_pairs = Vec::new();
    for py_rp in read_pairs.iter() {
        rust_read_pairs.push(py_to_rust_read_pair(&py_rp)?);
    }

    // Extract dedup parameters if provided
    let rust_dedup_params = if let Some(params) = dedup_params {
        let max_offset: usize = params.getattr("max_offset")?.extract()?;
        let max_error_frac: f64 = params.getattr("max_error_frac")?.extract()?;
        Some(DedupParams {
            max_offset,
            max_error_frac,
        })
    } else {
        None
    };

    // Extract minimizer parameters if provided
    let rust_minimizer_params = if let Some(params) = minimizer_params {
        let kmer_len: usize = params.getattr("kmer_len")?.extract()?;
        let window_len: usize = params.getattr("window_len")?.extract()?;
        let num_windows: usize = params.getattr("num_windows")?.extract()?;
        Some(
            MinimizerParams::new(kmer_len, window_len, num_windows)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))?
        )
    } else {
        None
    };

    if verbose {
        eprintln!(
            "Rust deduplication: processing {} read pairs",
            rust_read_pairs.len()
        );
    }

    // Run deduplication
    let result = deduplicate_read_pairs(rust_read_pairs, rust_dedup_params, rust_minimizer_params);

    // Convert result to Python dict
    let py_dict = PyDict::new_bound(py);
    for (read_id, exemplar_id) in result {
        py_dict.set_item(read_id, exemplar_id)?;
    }

    Ok(py_dict.unbind())
}

/// Python module definition
#[pymodule]
fn nao_dedup_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(deduplicate_read_pairs_rust, m)?)?;
    Ok(())
}
