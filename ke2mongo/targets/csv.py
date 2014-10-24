#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import luigi
from ke2mongo.log import log

class CSVTarget(luigi.LocalTarget):
    """
    Output a dataset df to CSV
    """
    def __init__(self, *args, **kwargs):

        # Remove custom kwargs before passing to luigi.LocalTarget.__init__()
        self.columns = kwargs.pop('columns')
        super(CSVTarget, self).__init__(*args, **kwargs)

    def write(self, df):

        row_count, col_count = df.shape

         # CSV Chunksize needs to be one more than block_size, so if we do get a UnicodeDecodeError, no rows will have been re-written
        chunksize = row_count + 1

        try:
            df.to_csv(self.path, chunksize=chunksize, mode='a', columns=self.columns.keys(), index=False, header=False, encoding='utf-8')
        except UnicodeDecodeError:

            # Batch writing to CSV failed - rather than ditch the whole batch, loop through and write each individually, logging an error for failures
            # Some of these failures are just corrupt records in KE EMu - for example record irn has
            # For example: DarFieldNumber:1=ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢

            # Loop through each row
            for i in range(row_count):
                # Get one row of the dataframe as new frame
                df_row = df[i:i+1]
                try:
                    # Try to write the row
                    df_row.to_csv(self.path, mode='a', columns=self.columns.keys(), index=False, header=False, encoding='utf-8')
                except UnicodeDecodeError:
                    # On failure, log an error with the _id of that row
                    log.critical('UTF8 Encoding error for record irn=%s', df_row.iloc[-1]['_id'])