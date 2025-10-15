CREATE TABLE gene_expression (
    sample_id         VARCHAR2(50) NOT NULL,
    gene_symbol       VARCHAR2(50) NOT NULL,
    expression_value  NUMBER,
    CONSTRAINT pk_expression PRIMARY KEY (sample_id, gene_symbol),
    CONSTRAINT fk_expression_sample
        FOREIGN KEY (sample_id)
        REFERENCES clinical(sample_id)
);
