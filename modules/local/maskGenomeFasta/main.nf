// Filter genomes to exclude specific patterns in sequence headers
process MASK_GENOME_FASTA {
    label "large"
    label "BBTools"
    input:
        path(filtered_genomes)
        path(adapters)
        val(params_map) // k, hdist, entropy, polyx_len, name_pattern
    output:
        path("${params_map.name_pattern}-masked.fasta.gz"), emit: masked
		path("${params_map.name_pattern}-mask-adapters-entropy.stats.txt"), emit: log1
		path("${params_map.name_pattern}-mask-polyx.stats.txt"), emit: log2
	script:
	// Extract parameters from map
	// Groovy runs first – build the poly-X literal once
	def polyx = ['A','C','G','T'].collect { it * (params_map.polyx_len as int) }.join(',')
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
		out=${params_map.name_pattern}-masked.fasta.gz \
		literal=${polyx} \
		stats=${params_map.name_pattern}-mask-polyx.stats.txt \
		k=${params_map.polyx_len} hdist=0 mm=f mask=N rcomp=F \
		-Xmx${task.memory.toGiga()}g
	"""
}