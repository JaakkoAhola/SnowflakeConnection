#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Jaakko Ahola
@company:  Virnex Group Oy
"""
import snowflake.connector

class ConnectSnowflake:
    def __init__(self, secrets):

        self.ctx = snowflake.connector.connect(account=secrets["account"],
                                               user=secrets["user"],
                                               password=secrets["password"],
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
