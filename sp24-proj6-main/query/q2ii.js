// Task 2ii

db.movies_metadata.aggregate([
    {
        $project: {
            tagline: { $toLower: "$tagline" }
        }
    },
    {
        $project: {
            words: { $split: ["$tagline", " "] }
        }
    },
    {
        $unwind: "$words"
    },
    {
        $project: {
            word: {
                $trim: {
                    input: "$words",
                    chars: ".,!?"
                }
            }
        }
    },
    {
        $match: {
            $expr: {
                $gt: [{ $strLenCP: "$word" }, 3]
            }
        }
    },
    {
        $group: {
            _id: "$word",
            count: { $sum: 1 }
        }
    },
    {
        $sort: { count: -1 }
    },
    {
        $limit: 20
    }
]);
