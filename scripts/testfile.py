"""
This code snippet is a unit test written in Python using the `unittest` framework. 
It is testing the `main` function of a script by mocking certain functions that the `main` function depends on. 
Here's a breakdown of what the code is doing:
"""

import unittest
from unittest.mock import patch

import main

class TestMainFunction(unittest.TestCase):
    
    @patch('scripts.fetch_data.fetch_companies')
    @patch('scripts.fetch_data.save_raw_data')
    @patch('scripts.analyze_data.analyze_financial_statements')
    def test_main(self, mock_analyze, mock_save, mock_fetch):
        # Setting up mocks
        mock_fetch.return_value = [{'name': 'Company1'}, {'name': 'Company2'}]
        
        # The main function is called when the script runs
        with patch('builtins.print') as mock_print:
            main.main()
            
            # Check if the functions were called correctly
            mock_fetch.assert_called_once()
            mock_save.assert_called_once_with([{'name': 'Company1'}, {'name': 'Company2'}])
            mock_analyze.assert_called_once()
            
            # Check if prints are as expected
            mock_print.assert_any_call("Starting main function")
            mock_print.assert_any_call("Main function completed")

if __name__ == "__main__":
    unittest.main()