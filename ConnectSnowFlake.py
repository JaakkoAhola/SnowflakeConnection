#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Jaakko Ahola
@company:  Virnex Group Oy
"""
import os
import sys
import snowflake.connector

sys.path.append(os.environ["LESMAINSCRIPTS"])
from FileSystem import FileSystem


class ConnectSnowflake:
    def __init__(self,
                 connection_name: str,
                 yaml_file="/home/jamesaloha/Virnex/Snowflake/connections.yaml"):

        configures = FileSystem.readYAML(yaml_file)
        configures_connection = configures[connection_name]

        self.ctx = snowflake.connector.connect(account=configures_connection["account"],
                                               user=configures_connection["user"],
                                               password=configures_connection["password"],
                                               )

        self.cursor = self.ctx.cursor()

    def get_ctx(self):
        return self.ctx

    def get_cursor(self):
        return self.cursor

    def close_ctx(self):
        self.ctx.close()

    def close_cursor(self):
        self.cursor.close()
