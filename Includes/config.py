# Databricks notebook source
import os
import json

def read_config():
    # Get the current working directory
    current_dir = os.getcwd()
    # Construct the full path to the config file
    config_path = os.path.join(current_dir, "Includes\\config\\config.json")

    with open(config_path, "r") as f:
        config = json.load(f)
    for i in config.keys():
        config[i] = os.path.join(current_dir, config[i])
    return config