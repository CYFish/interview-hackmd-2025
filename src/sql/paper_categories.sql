CREATE TABLE paper_categories (
    paper_id VARCHAR(20) REFERENCES papers(paper_id),
    category_id VARCHAR(50) REFERENCES categories(category_id),
    is_primary BOOLEAN,
    PRIMARY KEY (paper_id, category_id)
)
DISTKEY(paper_id)
SORTKEY(category_id);