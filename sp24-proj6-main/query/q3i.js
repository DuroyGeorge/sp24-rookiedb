// Task 3i

db.credits.aggregate([
    {
        $unwind: "$cast"
    },
    {
        $match: {
            "cast.id": 7624
        }
    },
    {
        $lookup: {
            from: "movies_metadata",
            localField: "movieId",
            foreignField: "movieId",
            as: "movie"
        }
    },
    {
        $project: {
            _id: 0,
            title: { $first: "$movie.title" },
            release_date: { $first: "$movie.release_date" },
            character: "$cast.character",
        }
    },
    {
        $sort: {
            release_date: -1
        }
    }
])