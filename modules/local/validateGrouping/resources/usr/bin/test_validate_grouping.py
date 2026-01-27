import pytest
from validate_grouping import validate_grouping


def write_tsv(path, header, rows):
    with open(path, 'w') as f:
        f.write('\t'.join(header) + '\n')
        for row in rows:
            f.write('\t'.join(row) + '\n')


def read_tsv_lines(path):
    """Read TSV file and return list of stripped non-empty lines."""
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


@pytest.fixture
def paths(tmp_path):
    """Create standard paths for test files."""
    return {
        'grouping': tmp_path / 'grouping.tsv',
        'virus_hits': tmp_path / 'virus_hits.tsv',
        'validated': tmp_path / 'validated_grouping.tsv',
        'partial': tmp_path / 'partial_group.tsv',
        'empty': tmp_path / 'empty_group.tsv',
    }


def run_validation(paths, grouping_rows, hits_samples):
    """Helper to set up files and run validation."""
    write_tsv(paths['grouping'], ['group', 'sample'], grouping_rows)
    write_tsv(paths['virus_hits'], ['sample'], [[s] for s in hits_samples])
    validate_grouping(
        str(paths['grouping']),
        str(paths['virus_hits']),
        str(paths['validated']),
        str(paths['partial']),
        str(paths['empty']),
    )
    return {
        'validated': read_tsv_lines(paths['validated']),
        'partial': read_tsv_lines(paths['partial']),
        'empty': read_tsv_lines(paths['empty']),
    }


class TestValidateGrouping:
    """Tests for validate_grouping function."""

    @pytest.mark.parametrize("grouping,hits,expected_validated,expected_partial,expected_empty", [
        # Sample in hits but not in grouping - should be ignored
        (
            [['g1', 'S1']],
            ['S1', 'S2'],
            {'g1\tS1'},
            set(),
            set(),
        ),
        # Sample without hits, group has other samples with hits -> partial
        (
            [['g1', 'S1'], ['g1', 'S2']],
            ['S1'],
            {'g1\tS1'},
            {'S2\tg1'},
            set(),
        ),
        # Sample without hits, entire group has no hits -> empty
        (
            [['g1', 'S1'], ['g2', 'S2']],
            ['S1'],
            {'g1\tS1'},
            set(),
            {'S2\tg2'},
        ),
        # All samples have hits - both partial and empty should be empty
        (
            [['g1', 'S1'], ['g1', 'S2'], ['g2', 'S3']],
            ['S1', 'S2', 'S3'],
            {'g1\tS1', 'g1\tS2', 'g2\tS3'},
            set(),
            set(),
        ),
        # Empty virus hits - all samples go to empty group
        (
            [['g1', 'S1'], ['g2', 'S2']],
            [],
            set(),
            set(),
            {'S1\tg1', 'S2\tg2'},
        ),
        # Mixed: partial and empty groups present
        # g1: S1 has hits, S2 no hits (partial)
        # g2: S3 no hits, S4 no hits (empty)
        # g3: S5 has hits (complete)
        (
            [['g1', 'S1'], ['g1', 'S2'], ['g2', 'S3'], ['g2', 'S4'], ['g3', 'S5']],
            ['S1', 'S5'],
            {'g1\tS1', 'g3\tS5'},
            {'S2\tg1'},
            {'S3\tg2', 'S4\tg2'},
        ),
        # Multiple samples in same empty group
        (
            [['g1', 'S1'], ['g1', 'S2'], ['g1', 'S3']],
            [],
            set(),
            set(),
            {'S1\tg1', 'S2\tg1', 'S3\tg1'},
        ),
    ])
    def test_categorization(self, paths, grouping, hits, expected_validated, expected_partial, expected_empty):
        """Test that samples are correctly categorized into validated, partial, and empty outputs."""
        result = run_validation(paths, grouping, hits)

        # Check validated output
        assert result['validated'][0] == 'group\tsample'
        assert set(result['validated'][1:]) == expected_validated

        # Check partial group output
        assert result['partial'][0] == 'sample\tgroup'
        assert set(result['partial'][1:]) == expected_partial

        # Check empty group output
        assert result['empty'][0] == 'sample\tgroup'
        assert set(result['empty'][1:]) == expected_empty
