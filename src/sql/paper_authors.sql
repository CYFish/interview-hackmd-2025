CREATE TABLE paper_authors (
    paper_id VARCHAR(20) REFERENCES papers(paper_id),
    author_id VARCHAR(50) REFERENCES authors(author_id),
    author_position INTEGER,
    PRIMARY KEY (paper_id, author_id)
)
DISTKEY(paper_id)
SORTKEY(author_position);