from cuallee.helper_functions import _delete_dict_entry

def test_delete_dict_entry():
    first_dict = {'abc' : 1, 'def': 2, 'ghi': 3}
    second_dict = {'abc' : 'add_numbers', 'def': "delete_numbers"}
    _delete_dict_entry(['def', 'ghi'], first_dict, second_dict)
    assert len(first_dict) == 1
    assert len (second_dict) == 1
