# """
# # Copyright 2017 Foundation Center. All Rights Reserved.
# #
# # Licensed under the Foundation Center Public License, Version 1.1 (the “License”);
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #     http://gis.foundationcenter.org/licenses/LICENSE-1.1.html
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an “AS IS” BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# # ==============================================================================
# """
# import pymysql
#
#
# class MySqlDataBase:
#
#     def __init__(self, connection_dict):
#         self.connection = None
#         self.cursor = self._connect(connection_dict)
#
#     def _connect(self, c):
#         self.connection = pymysql.connect(host=c["SERVER"],
#                                           user=c["UID"],
#                                           password=c["PWD"],
#                                           db=c["DATABASE"],
#                                           port=int(c["PORT"]),
#                                           charset="utf8",
#                                           cursorclass=pymysql.cursors.DictCursor)
#         return self.connection.cursor()
#
#     def execute(self, query, silent=False):
#         if silent:
#             self.cursor.execute(query)
#             self.cursor.commit()
#             return True
#         return self.cursor.execute(query)
#
#     def execute_many(self, query, array):
#         self.cursor.executemany(query, array)
#         self.connection.commit()
#
#     def teardown(self):
#         """
#         Closes all connections and cursors
#         """
#         self.cursor.close()
#         self.connection.close()
