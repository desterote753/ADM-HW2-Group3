import functions
import processing

data_path = functions.get_from_config("harddrive")
processing.initialize_processing_parameters_from_config()
### RQ_7_1

# print(processing.answer_rq_7_1(data_path))

# print(processing.get_worst_book_ids_of_all_time(data_path))

worst_book_ids_of_all_time_set = processing.get_worst_book_ids_of_all_time(data_path)
contingency_table = processing.get_contingency_table_for_rq_7_3(data_path, worst_book_ids_of_all_time_set)
# processing.answer_rq_7_3(700, 1000)