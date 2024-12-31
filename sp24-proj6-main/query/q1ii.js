// Task 1ii

db.movies_metadata.aggregate([
    {
        $match: {
            vote_count: { $gte: 50 },
            genres: { $elemMatch: { name: "Comedy" } }
        }
    },
    {
        $sort: { vote_average: -1, vote_count: -1, movieId: 1 }
    },
    {
        $project: {
            _id: 0,
            title: 1,
            vote_average: 1,
            vote_count: 1,
            movieId: 1,
        }
    },
    {
        $limit: 50
    }
]);