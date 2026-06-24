// Filter genomes to exclude specific patterns in sequence headers
process MASK_GENOME_FASTA {
    label "large"
    label "BBTools"
    tag "id=index"
    input:
        path(filtered_genomes)
        path(adapters)
        path(ribo_ref)
        val(params_map) // k, hdist, entropy, polyx_len, nucleaze_k, name_pattern
    output:
        path("${params_map.name_pattern}-masked.fasta.gz"), emit: masked
		path("${params_map.name_pattern}-mask-adapters-entropy.stats.txt"), emit: log1
		path("${params_map.name_pattern}-mask-polyx.stats.txt"), emit: log2
		path("${params_map.name_pattern}-mask-rrna.stats.txt"), emit: log3
	script:
	// Extract parameters from map
	// Groovy runs first – build the poly-X literal once
	def polyx = ['A','C','G','T'].collect { base -> base * (params_map.polyx_len as int) }.join(',')
	"""
	# first pass – adapters + entropy
	bbduk.sh \
		in=${filtered_genomes} \
		out=intermediate-masking.fasta.gz \
		ref=${adapters} \
		stats=${params_map.name_pattern}-mask-adapters-entropy.stats.txt \
		k=${params_map.k} hdist=${params_map.hdist} mm=f mask=N rcomp=t \
		entropy=${params_map.entropy} entropymask=t mink=8 hdist2=1 \
		-Xmx${task.memory.toGiga()}g

	# second pass – poly-X masking
	bbduk.sh \
		in=intermediate-masking.fasta.gz \
		out=intermediate-masking-polyx.fasta.gz \
		literal=${polyx} \
		stats=${params_map.name_pattern}-mask-polyx.stats.txt \
		k=${params_map.polyx_len} hdist=0 mm=f mask=N rcomp=F \
		-Xmx${task.memory.toGiga()}g

	# third pass – host rRNA (ribo-ref used directly; BBDuk treats U as T).
	# k is the EXTRACT_VIRAL_READS k-mer screen size (nucleaze_k): masking at the
	# screen's k removes exactly the rRNA k-mers it would otherwise index, while a
	# smaller k would over-mask (also eroding the bowtie2/minimap2 alignment ref).
	# hdist=0 (exact) keeps masking to genuine rRNA matches.
	bbduk.sh \
		in=intermediate-masking-polyx.fasta.gz \
		out=${params_map.name_pattern}-masked.fasta.gz \
		ref=${ribo_ref} \
		stats=${params_map.name_pattern}-mask-rrna.stats.txt \
		k=${params_map.nucleaze_k} hdist=0 mm=f mask=N rcomp=t \
		-Xmx${task.memory.toGiga()}g
	"""
}
