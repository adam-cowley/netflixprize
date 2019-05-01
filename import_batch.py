import fileinput
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo"))

# Movies: 4499
# Reviews: 24053765

# Stats
movies = 0
reviews = 0

# Import
movie_id = False
reviews = []

query = """
UNWIND $batch AS row
MATCH (m:Movie {id: row.movie_id})
UNWIND row.reviews AS review
MERGE (u:User {id: review.user_id})
MERGE (u)-[r:RATED]->(m)
SET r.rating = toFloat(review.rating), r.date = date(review.date)

RETURN count(*) as count
"""

class Importer:
    def __init__(self, driver, batch_size=5000):
        # Config
        self.driver = driver
        self.batch_size = batch_size

        self.session = driver.session()

        # Statistics
        self.movies = 0
        self.reviews = 0
        self.batches = 0

        # Batching
        self.movie_id = False
        self.batch = {}

    def read_line(self, line):
        if ":" in line:
            self.check_batch()

            self.movies = self.movies + 1

            parts = line.split(":")
            self.movie_id = parts[0]

            self.batch[self.movie_id] = { "movie_id": self.movie_id, "reviews": [] }
        else:
            self.reviews = self.reviews + 1

            parts = line.split(",")

            self.batch[ self.movie_id ]["reviews"].append({
                "movie_id": self.movie_id,
                "user_id": parts[0],
                "rating": parts[1],
                "type": parts[2]
            })

    def check_batch(self):
        if len(self.batch) >= self.batch_size:
            self.execute_batch()

    def execute_batch(self):
        self.batches = self.batches + 1

        print("sending batch number", self.batches, len(self.batch), self.reviews)

    
        res = self.session.run(query, batch= list(self.batch.values()) )

        print(res.single().get("count"))

        self.batch = {}

        # print("new batch", len(self.batch))

    def import_reviews(self, filename):
        for line in fileinput.input(filename):
            self.read_line(line)

        self.execute_batch()

        print("Movies ", self.movies)
        print("Ratings", self.reviews)
        print("Batches", self.batches)


res = Importer(driver, batch_size=1)
res.import_reviews("import/combined_data_2.txt")