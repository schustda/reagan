import pandas as pd
from configparser import ConfigParser
from reagan.subclass import Subclass
import smartsheet


class SmartsheetAPI(Subclass):
    def __init__(self):
        super().__init__()
        self.bearer_token = self.get_parameter_value("/smartsheets/bearer_token")
        self.conn = smartsheet.Smartsheet(self.bearer_token)

    def get_attachment_url(self, sheet_id, attachment_id):
        """
        Inputs - sheet_id (int), attachment_id (int)
        Returns - url of attachment generated by smartsheet API. (Only lasts for 2min)
        """

        attachments_obj = self.conn.Attachments.get_attachment(sheet_id, attachment_id)
        a = attachments_obj.to_dict()
        if a.get("url"):
            return a.get("url")
        else:
            self.vprint("No url returned: {a}")

    def attachment_to_dict(self, sheet_id, attachment_id):
        """
        Use only for excel attachments

        Inputs - sheet_id (int), attachment_id (int)
        Returns - Dictionary Object: key = Sheet Name, value = DataFrame 
        """

        url = self.get_attachment_url(sheet_id, attachment_id)
        excel_sheet_names = pd.ExcelFile(url).sheet_names
        attachment = {}
        for sheet_name in excel_sheet_names:
            df = pd.read_excel(url, sheet_name=sheet_name)
            attachment[sheet_name] = df
        return attachment

    def discussions_to_df(self, sheet_id):
        discussions_obj = self.conn.Discussions.get_all_discussions(
            sheet_id, include_all=True, include=["comments", "attachments"]
        )
        data = discussions_obj.to_dict()

        # Check that data exists else return empty df
        if "data" not in data.keys():
            return pd.DataFrame()

        disc = self._json_to_df(data["data"])
        disc["sheet_id"] = sheet_id

        return disc

    def sheet_to_df(self, sheet_id):
        # Make API Call
        sheet_obj = self.conn.Sheets.get_sheet(sheet_id)

        # Sheet columns to use
        other_df_cols = ["createdAt", "id", "modifiedAt", "name", "permalink"]

        # Get the data in dict form
        data = sheet_obj.to_dict()

        # Check that data exists else return empty df
        for x in ["columns", "rows"]:
            if x not in data.keys():
                return pd.DataFrame()

        # Get the columns first
        columns_dict = {}
        columns_dict["columns"] = data["columns"]
        columns_df = self._json_to_df(columns_dict)
        columns_df = columns_df[["columns_id", "columns_title"]].drop_duplicates()

        # Get the acutal data
        rows_dict = {}
        rows_dict["rows"] = data["rows"]
        rows_df = self._json_to_df(rows_dict)

        df = pd.merge(
            rows_df,
            columns_df,
            left_on="rows_cells_columnId",
            right_on="columns_id",
            how="left",
        )

        for col in other_df_cols:
            df[col] = data[col]

        return df
