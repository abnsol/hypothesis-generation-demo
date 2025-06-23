import json
import pickle
from pengines.Builder import PengineBuilder
from pengines.Pengine import Pengine
from loguru import logger

class PrologQuery:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        logger.info(f"Connecting to Prolog server at {host}:{port}")
        self.pengine_builder = PengineBuilder(urlserver=f"http://{self.host}:{self.port}")
        # self.pengine = Pengine(builder=pengine_builder)
        # self.pengine.create()

    def get_candidate_genes(self, variant_id):
        """
        Given a SNP, get candidate genes that are proximal to it.
        """
        logger.info(f"Getting candidate genes for variant {variant_id}")
        query = f"candidate_genes(snp({variant_id}), Genes)"
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        
        # Check if the query succeeded
        if pengine.currentQuery is None:
            logger.error(f"Failed to get candidate genes for variant {variant_id}")
            return []
        
        # Check if there are any proofs available
        if not pengine.currentQuery.availProofs:
            logger.error(f"No candidate genes found for variant {variant_id}")
            return []
        
        result = pengine.currentQuery.availProofs[0]
        if "Genes" not in result:
            logger.error(f"Expected 'Genes' key not found in result for variant {variant_id}")
            return []
        
        genes = [g.upper() for g in result["Genes"]]
        return genes
    
    def get_relevant_gene_proof(self, variant_id, gene):
        gene_ids = self.get_gene_ids([gene])
        if not gene_ids or gene_ids[0] is None:
            logger.error(f"Failed to get gene ID for gene {gene}")
            return None, f"Gene {gene} not found in knowledge base"
        
        gene_id = gene_ids[0]
        query = f"json_proof_tree(relevant_gene(gene({gene_id}), snp({variant_id})), Graph)"
        print(f"Gene Proof Query: {query}")
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        
        if pengine.currentQuery is None or not pengine.currentQuery.availProofs:
            # Try to get the rule body to understand why it failed
            try:
                pengine = Pengine(builder=self.pengine_builder)
                pengine.doAsk(pengine.ask("meta_interpreter:rule_body(relevant_gene(G, S), R)"))
                if pengine.currentQuery and pengine.currentQuery.availProofs:
                    rule = pengine.currentQuery.availProofs[0]["R"]
                    print(f"q rule: {rule}")
                    return None, rule
                else:
                    logger.error(f"Failed to get rule body for relevant_gene query")
                    return None, "Failed to determine why gene proof query failed"
            except Exception as e:
                logger.error(f"Error getting rule body: {str(e)}")
                return None, f"Gene proof query failed for {gene} and {variant_id}"
        
        # Check if the Graph key exists in the result
        if "Graph" not in pengine.currentQuery.availProofs[0]:
            logger.error(f"Graph key not found in relevant gene proof result for {gene} and {variant_id}")
            return None, f"Invalid graph structure returned for {gene} and {variant_id}"
        
        graph = pengine.currentQuery.availProofs[0]["Graph"]
        try:
            return json.loads(graph), True
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse graph JSON for {gene} and {variant_id}: {str(e)}")
            return None, f"Invalid graph format returned for {gene} and {variant_id}"
    
    def get_gene_ids(self, genes):
        genes = [g.lower() for g in genes]
        query = f"maplist(gene_id, {genes}, GeneIds)"
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        
        # Check if the query succeeded
        if pengine.currentQuery is None:
            logger.error(f"Failed to get gene IDs for genes: {genes}")
            return []
        
        # Check if there are any proofs available
        if not pengine.currentQuery.availProofs:
            logger.error(f"No gene IDs found for genes: {genes}")
            return []
        
        if "GeneIds" not in pengine.currentQuery.availProofs[0]:
            logger.error(f"Expected 'GeneIds' key not found in result for genes: {genes}")
            return []
        
        gene_ids = pengine.currentQuery.availProofs[0]["GeneIds"]
        # self.pengine.doStop()
        return gene_ids

    def execute_query(self, query):
        pengine = Pengine(builder=self.pengine_builder)
        pengine.doAsk(pengine.ask(query))
        
        # Check if the query succeeded
        if pengine.currentQuery is None:
            logger.error(f"Prolog query failed: {query}")
            return None
        
        # Check if there are any proofs available
        if not pengine.currentQuery.availProofs:
            logger.error(f"Prolog query returned no results: {query}")
            return None
        
        # Check if the expected key exists in the result
        if "X" not in pengine.currentQuery.availProofs[0]:
            logger.error(f"Prolog query result missing expected key 'X': {query}")
            return None
        
        return pengine.currentQuery.availProofs[0]["X"]

if __name__ == "__main__":
    prolog_query = PrologQuery(host="100.67.47.42", port=4242)
    result = prolog_query.execute_query("maplist(variant_id, [snp(rs1421085)], X)")
#     # result = prolog_query.get_candidate_genes("rs1421085")
    print(result)
