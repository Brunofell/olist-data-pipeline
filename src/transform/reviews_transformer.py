import pandas as pd


class ReviewsTransformer:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def convert_dates(self) -> None:

        date_columns = [
            "review_creation_date",
            "review_answer_timestamp"
        ]

        for column in date_columns:
            self.df[column] = pd.to_datetime(
                self.df[column],
                errors="coerce"
            )

    def normalize_text(self) -> None:

        text_columns = [
            "review_comment_title",
            "review_comment_message"
        ]

        for column in text_columns:

            self.df[column] = (
                self.df[column]
                .str.strip()
            )

            self.df[column] = (
                self.df[column]
                .replace("", pd.NA)
            )

    def create_comment_flag(self) -> None:

        self.df["has_comment"] = (
            self.df["review_comment_message"]
            .notna()
        )

    def validate_review_score(self) -> None:

        if not self.df["review_score"].between(1, 5).all():
            raise ValueError(
                "Invalid review score detected."
            )

    def run(self) -> pd.DataFrame:

        self.convert_dates()
        self.normalize_text()
        self.create_comment_flag()
        self.validate_review_score()

        return self.df