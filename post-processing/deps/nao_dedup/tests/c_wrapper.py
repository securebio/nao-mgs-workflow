"""
ctypes wrapper for the C nao_dedup library.

This allows Python tests to call C functions directly.
"""

import ctypes
import os
from pathlib import Path

# Find the shared library
lib_path = Path(__file__).parent / "libnaodedup_test.so"
if not lib_path.exists():
    # Try building it
    raise FileNotFoundError(
        f"Test library not found at {lib_path}. "
        f"Run 'make test-lib'."
    )

lib = ctypes.CDLL(str(lib_path))

# ============================================================================
# Type definitions matching C structs
# ============================================================================

class NaoDedupParams(ctypes.Structure):
    _fields_ = [
        ("kmer_len", ctypes.c_int),
        ("window_len", ctypes.c_int),
        ("num_windows", ctypes.c_int),
        ("max_offset", ctypes.c_int),
        ("max_error_frac", ctypes.c_double),
        ("expected_reads", ctypes.c_size_t),
        ("scratch_arena_size", ctypes.c_size_t),
        ("result_arena_size", ctypes.c_size_t),
    ]

class NaoDedupStats(ctypes.Structure):
    _fields_ = [
        ("total_reads_processed", ctypes.c_int),
        ("unique_clusters", ctypes.c_int),
        ("scratch_arena_used", ctypes.c_size_t),
        ("result_arena_used", ctypes.c_size_t),
    ]

# ============================================================================
# Function signatures
# ============================================================================

lib.nao_dedup_create.argtypes = [NaoDedupParams]
lib.nao_dedup_create.restype = ctypes.c_void_p

lib.nao_dedup_destroy.argtypes = [ctypes.c_void_p]
lib.nao_dedup_destroy.restype = None

lib.nao_dedup_get_error_message.argtypes = []
lib.nao_dedup_get_error_message.restype = ctypes.c_char_p

lib.nao_dedup_process_read.argtypes = [
    ctypes.c_void_p,     # ctx
    ctypes.c_char_p,     # read_id
    ctypes.c_size_t,     # id_len
    ctypes.c_char_p,     # fwd_seq
    ctypes.c_size_t,     # fwd_len
    ctypes.c_char_p,     # rev_seq
    ctypes.c_size_t,     # rev_len
    ctypes.c_char_p,     # fwd_qual
    ctypes.c_size_t,     # fwd_qual_len
    ctypes.c_char_p,     # rev_qual
    ctypes.c_size_t,     # rev_qual_len
]
lib.nao_dedup_process_read.restype = ctypes.c_char_p

lib.nao_dedup_finalize.argtypes = [ctypes.c_void_p]
lib.nao_dedup_finalize.restype = None

lib.nao_dedup_get_final_exemplar.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
lib.nao_dedup_get_final_exemplar.restype = ctypes.c_char_p

lib.nao_dedup_get_stats.argtypes = [ctypes.c_void_p, ctypes.POINTER(NaoDedupStats)]
lib.nao_dedup_get_stats.restype = None

# ============================================================================
# Python wrapper functions
# ============================================================================

class NaoDedupContext:
    """Python wrapper for NaoDedupContext."""

    def __init__(self, params_dict):
        """Create context from parameter dictionary."""
        params = NaoDedupParams(
            kmer_len=params_dict.get('kmer_len', 15),
            window_len=params_dict.get('window_len', 25),
            num_windows=params_dict.get('num_windows', 4),
            max_offset=params_dict.get('max_offset', 1),
            max_error_frac=params_dict.get('max_error_frac', 0.01),
            expected_reads=params_dict.get('expected_reads', 1000000),
            scratch_arena_size=params_dict.get('scratch_arena_size', 0),  # 0 = use default (2GB)
            result_arena_size=params_dict.get('result_arena_size', 0),    # 0 = use default (512MB)
        )

        self._ctx = lib.nao_dedup_create(params)
        if not self._ctx:
            error_msg = lib.nao_dedup_get_error_message()
            raise RuntimeError(f"Failed to create context: {error_msg.decode()}")

    def process_read(self, read_id, fwd_seq, rev_seq, fwd_qual=None, rev_qual=None):
        """Process a read and return its exemplar."""
        read_id_enc = read_id.encode()
        fwd_seq_enc = fwd_seq.encode()
        rev_seq_enc = rev_seq.encode()
        fwd_qual_enc = fwd_qual.encode() if fwd_qual else b''
        rev_qual_enc = rev_qual.encode() if rev_qual else b''

        result = lib.nao_dedup_process_read(
            self._ctx,
            read_id_enc, len(read_id_enc),
            fwd_seq_enc, len(fwd_seq_enc),
            rev_seq_enc, len(rev_seq_enc),
            fwd_qual_enc if fwd_qual else None, len(fwd_qual_enc),
            rev_qual_enc if rev_qual else None, len(rev_qual_enc),
        )
        return result.decode()

    def finalize(self):
        """Finalize Pass 1."""
        lib.nao_dedup_finalize(self._ctx)

    def get_final_exemplar(self, read_id):
        """Get final exemplar for a read."""
        result = lib.nao_dedup_get_final_exemplar(self._ctx, read_id.encode())
        return result.decode()

    def get_stats(self):
        """Get statistics."""
        stats = NaoDedupStats()
        lib.nao_dedup_get_stats(self._ctx, ctypes.byref(stats))
        return {
            'total_reads_processed': stats.total_reads_processed,
            'unique_clusters': stats.unique_clusters,
            'scratch_arena_used': stats.scratch_arena_used,
            'result_arena_used': stats.result_arena_used,
        }

    def __del__(self):
        """Clean up context."""
        if hasattr(self, '_ctx') and self._ctx:
            lib.nao_dedup_destroy(self._ctx)


def deduplicate_read_pairs_c(read_pairs, dedup_params=None, minimizer_params=None, verbose=False):
    """
    C implementation compatible with Python deduplicate_read_pairs_streaming API.

    Returns dict mapping read_id -> exemplar_id
    """
    from dedup import DedupParams, MinimizerParams

    if dedup_params is None:
        dedup_params = DedupParams()
    if minimizer_params is None:
        minimizer_params = MinimizerParams()

    # Create context with parameters
    params_dict = {
        'kmer_len': minimizer_params.kmer_len,
        'window_len': minimizer_params.window_len,
        'num_windows': minimizer_params.num_windows,
        'max_offset': dedup_params.max_offset,
        'max_error_frac': dedup_params.max_error_frac,
        'expected_reads': len(read_pairs) if read_pairs else 1000,
    }

    ctx = NaoDedupContext(params_dict)

    # Process all reads
    result = {}
    for rp in read_pairs:
        # ReadPair only stores mean_q (a single float), not original quality
        # strings, but the API we're testing wants the string.  So we can test
        # it, construct uniform quality strings thay will produce the same mean
        # quality.
        qual_char = chr(int(rp.mean_q) + 33)
        fwd_qual = qual_char * len(rp.fwd_seq)
        rev_qual = qual_char * len(rp.rev_seq)

        exemplar = ctx.process_read(
            rp.read_id,
            rp.fwd_seq,
            rp.rev_seq,
            fwd_qual,
            rev_qual,
        )
        result[rp.read_id] = exemplar

    ctx.finalize()

    final_result = {}
    for read_id in result:
        final_exemplar = ctx.get_final_exemplar(read_id)
        final_result[read_id] = final_exemplar

    if verbose:
        stats = ctx.get_stats()
        print(f"C dedup: {stats['total_reads_processed']} reads, "
              f"{stats['unique_clusters']} clusters")

    return final_result
