CREATE TABLE papers (
    paper_id VARCHAR(20) PRIMARY KEY,
    submitted_date DATE,
    published_date DATE,
    journal_ref VARCHAR(256),
    version_count INTEGER,
    submission_to_publication_days INTEGER,
    title VARCHAR(1024)
)
DISTKEY(paper_id)
SORTKEY(submitted_date);