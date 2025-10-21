mock_causal_graph = {

    "nodes": [

        {"id": "rs1421085", "type": "snp"},
        {"id": "ensg00000177508", "type": "gene"},
        {"id": "rs1421085", "type": "snp"},
        {"id": "ensg00000177508", "type": "gene"},
        {"id": "rs1421085", "type": "snp"},
        {"id": "chr16_53741418_53785410_grch38", "type": "super_enhancer"},
        {"id": "chr16_53741418_53785410_grch38", "type": "super_enhancer"},
        {"id": "ensg00000140718", "type": "gene"},
        {"id": "rs1421085", "type": "snp"},
        {"id": "ensg00000125798", "type": "gene"},
        {"id": "ensg00000125798", "type": "gene"},
        {"id": "ensg00000177508", "type": "gene"},
        {"id": "ensg00000125798", "type": "gene"},
        {"id": "chr16_53744537_53744917_grch38", "type": "tfbs"},
        {"id": "chr16_53744537_53744917_grch38", "type": "tfbs"},
        {"id": "chr16_53741418_53785410_grch38", "type": "super_enhancer"},
    ],
    "edges": [
        {
            "source": "rs1421085",
            "target": "ensg00000177508",
            "label": "eqtl_association",
        },
        {"source": "rs1421085", "target": "ensg00000177508", "label": "in_tad_with"},
        {
            "source": "rs1421085",
            "target": "chr16_53741418_53785410_grch38",
            "label": "in_regulatory_region",
        },
        {
            "source": "chr16_53741418_53785410_grch38",
            "target": "ensg00000140718",
            "label": "associated_with",
        },
        {"source": "rs1421085", "target": "ensg00000125798", "label": "alters_tfbs"},
        {
            "source": "ensg00000125798",
            "target": "ensg00000177508",
            "label": "regulates",
        },
        {
            "source": "ensg00000125798",
            "target": "chr16_53744537_53744917_grch38",
            "label": "binds_to",
        },
        {
            "source": "chr16_53744537_53744917_grch38",
            "target": "chr16_53741418_53785410_grch38",
            "label": "overlaps_with",
        },
    ],
}

def get_mock_causal_graph():
    return mock_causal_graph