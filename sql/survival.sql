CREATE TABLE survival (
    sample_id       VARCHAR2(50) NOT NULL PRIMARY KEY,
    os              NUMBER,
    os_time         NUMBER,
    dss             NUMBER,
    dss_time        NUMBER,
    dfi             NUMBER,
    dfi_time        NUMBER,
    pfi             NUMBER,
    pfi_time        NUMBER,
    redaction       VARCHAR2(50),
    CONSTRAINT fk_survival_sample
        FOREIGN KEY (sample_id)
        REFERENCES clinical(sample_id)
);
