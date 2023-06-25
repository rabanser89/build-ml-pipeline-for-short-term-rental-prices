#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifactDownload from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path)

    logger.info("Drop outliers")
    #Drop if price is not within range given by min_price and max_price
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    logger.info("Convert last_review to datetime")
    #Change date format from string to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info("Longitude and latitude boundaries for properties in and around NYC")
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file("clean_sample.csv")
    logger.info("Logging artifact")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified artifact name for the input data",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Fully-qualified artifact name for the output data",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Name of the output type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="output description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum value for prices",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum value for prices",
        required=True
    )


    args = parser.parse_args()

    go(args)
