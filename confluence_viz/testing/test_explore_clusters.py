# description: Tests the explore_clusters module.

'''
Test suite for explore_clusters.py
'''
import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import sys
from datetime import datetime

# Assuming explore_clusters.py is in the same directory as this test file.
# If not, sys.path might need adjustment for the import to work.
import explore_clusters

class TestSearchApplicationsIndexedAllSpacesPerTerm(unittest.TestCase):
    '''
    Tests for the search_applications_indexed_all_spaces_per_term function.
    '''

    # Patch globals assumed to be in explore_clusters.py
    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('explore_clusters.WHOOSH_INDEX_DIR', 'dummy_whoosh_dir')
    @patch('whoosh.index.exists_in') # Patched at source
    @patch('explore_clusters.os.path.exists')
    @patch('builtins.print')
    def test_missing_whoosh_index_dir(self, mock_print, mock_os_path_exists, mock_whoosh_exists_in, _mock_whoosh_index_dir_global, _mock_whoosh_available): # Adjusted args
        '''Test behavior when Whoosh index directory is missing.'''
        # Simulate Whoosh index directory not existing
        mock_os_path_exists.return_value = True # Let's say the path itself exists
        mock_whoosh_exists_in.return_value = False     # But it's not a valid index

        explore_clusters.search_applications_indexed_all_spaces_per_term()

        mock_print.assert_any_call("Error: Whoosh index not found in dummy_whoosh_dir")
        mock_print.assert_any_call("Please run option 14 first to create the search index.")

    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('explore_clusters.WHOOSH_INDEX_DIR', 'dummy_whoosh_dir')
    @patch('whoosh.index.exists_in', return_value=True) # Patched at source
    @patch('explore_clusters.os.path.dirname', return_value='dummy_root_dir')
    @patch('explore_clusters.os.path.exists')
    @patch('builtins.print')
    def test_missing_app_search_txt(self, mock_print, mock_os_path_exists, mock_os_path_dirname, mock_whoosh_exists_in, _mock_whoosh_index_dir_global, _mock_whoosh_available): # Adjusted args
        '''Test behavior when app_search.txt is missing.'''
        app_search_path = os.path.join('dummy_root_dir', 'app_search.txt')

        def os_path_exists_side_effect(path):
            if path == 'dummy_whoosh_dir':
                return True
            if path == app_search_path:
                return False # app_search.txt does not exist
            return True # Default for other paths
        mock_os_path_exists.side_effect = os_path_exists_side_effect

        explore_clusters.search_applications_indexed_all_spaces_per_term()

        mock_print.assert_any_call(f"Error: app_search.txt not found at {app_search_path}")
        mock_print.assert_any_call("Please create this file with one application name per line.")

    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('explore_clusters.WHOOSH_INDEX_DIR', 'dummy_whoosh_dir')
    @patch('whoosh.index.exists_in', return_value=True) # Patched at source
    @patch('explore_clusters.os.path.exists', return_value=True) # All paths exist
    @patch('explore_clusters.os.path.dirname', return_value='dummy_root_dir')
    @patch('builtins.open', new_callable=mock_open)
    @patch('whoosh.index.open_dir') # Patched at source
    @patch('whoosh.qparser.MultifieldParser') # Patched at source
    @patch('whoosh.qparser.OrGroup', new_callable=MagicMock) # Patched at source
    @patch('html.escape', side_effect=lambda x: x) # Patched at source
    @patch('explore_clusters.datetime')
    @patch('explore_clusters.webbrowser.open')
    @patch('builtins.print')
    def test_successful_run_no_hits(self, mock_print, mock_webbrowser_open, mock_datetime_module,
                                    mock_html_escape, mock_OrGroup, mock_MultifieldParser, mock_whoosh_open_dir,
                                    mock_builtin_open_constructor, mock_os_path_exists, mock_os_path_dirname,
                                    mock_whoosh_exists_in, _mock_whoosh_index_dir_global, _mock_whoosh_available): # Adjusted args
        '''Test a successful run with search terms but no Whoosh hits.'''
        app_search_content = "# Comment\n\nSearch Term 1\nSearch Term 2\nSpecialChars:+-Term"
        # Mock for reading app_search.txt and writing HTML file
        mock_app_search_file = mock_open(read_data=app_search_content).return_value
        mock_html_file = mock_open().return_value
        mock_builtin_open_constructor.side_effect = [mock_app_search_file, mock_html_file]

        mock_ix = mock_whoosh_open_dir.return_value
        mock_ix.schema = MagicMock()
        mock_searcher_context = mock_ix.searcher.return_value
        mock_searcher = mock_searcher_context.__enter__.return_value
        mock_parser_instance = mock_MultifieldParser.return_value
        mock_parser_instance.parse.return_value = "parsed_query_object"
        mock_searcher.search.return_value = [] # No Whoosh hits

        # Mock datetime.now() calls
        dt_start = datetime(2023, 1, 1, 12, 0, 0)
        dt_end_loop = datetime(2023, 1, 1, 12, 0, 1)
        dt_timestamp_report = datetime(2023, 1, 1, 12, 0, 5)
        mock_datetime_module.now.side_effect = [dt_start, dt_end_loop, dt_timestamp_report]
        # Ensure datetime subtraction works as expected for timedelta
        mock_datetime_module.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

        explore_clusters.search_applications_indexed_all_spaces_per_term()

        expected_app_search_path = os.path.join('dummy_root_dir', 'app_search.txt')
        mock_builtin_open_constructor.assert_any_call(expected_app_search_path, 'r', encoding='utf-8')
        mock_builtin_open_constructor.assert_any_call('all_spaces_per_term_application_search_results.html', 'w', encoding='utf-8')

        mock_print.assert_any_call("Loaded 3 application search terms from app_search.txt")
        mock_print.assert_any_call('Processing term 1/3: "Search Term 1" ')
        mock_print.assert_any_call('Processing term 2/3: "Search Term 2" ')
        mock_print.assert_any_call('Processing term 3/3: "SpecialChars:+-Term" ')

        mock_MultifieldParser.assert_any_call(["page_title", "page_content"], mock_ix.schema, group=mock_OrGroup)
        self.assertEqual(mock_searcher.search.call_count, 3)
        mock_searcher.search.assert_any_call("parsed_query_object", limit=10000)

        self.assertTrue(mock_html_file.write.called)
        mock_print.assert_any_call("\nSearch complete! Results written to all_spaces_per_term_application_search_results.html")
        # Note: os.path.abspath will run on the actual machine, so the path might vary if tests run in different CWDs.
        # For more robust testing of this, consider mocking os.path.abspath as well.
        mock_webbrowser_open.assert_called_with('file://' + os.path.abspath('all_spaces_per_term_application_search_results.html'))


class TestMainMenu(unittest.TestCase):
    '''
    Tests for the main menu and option dispatching in explore_clusters.py
    '''

    @patch('explore_clusters.WHOOSH_AVAILABLE', True) # Common dependency
    @patch('builtins.input')
    @patch('explore_clusters.search_applications_indexed_all_spaces_per_term')
    @patch('builtins.print') # To suppress other prints from main loop
    def test_menu_option_search_all_spaces_and_quit(self, mock_print, mock_search_all_spaces, mock_input, _mock_whoosh_available): # Adjusted args
        '''
        Test selecting option '15' for search_applications_indexed_all_spaces_per_term
        and then quitting.
        '''
        # Simulate user entering '15' then 'q'
        mock_input.side_effect = ['15', 'q']

        # Call the main function which contains the menu loop
        try:
            explore_clusters.main()
        except SystemExit: # main calls sys.exit() on 'q'
            pass

        mock_search_all_spaces.assert_called_once()
        mock_input.assert_any_call("\nEnter your choice (or 'h' for help, 'q' to quit): ")

    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('builtins.input')
    @patch('builtins.print') # To suppress other prints and capture filter setting prints
    def test_menu_option_set_space_key_filter(self, mock_print, mock_input, _mock_whoosh_available): # Adjusted args
        '''Test selecting option '1' to set the space key filter.'''
        mock_input.side_effect = ['1', 'TESTKEY', 'q']

        try:
            explore_clusters.main()
        except SystemExit:
            pass

        mock_input.assert_any_call("\nEnter your choice (or 'h' for help, 'q' to quit): ")
        mock_input.assert_any_call("Enter space key to filter by (e.g., ITS, OPS), or leave blank to clear: ")
        mock_print.assert_any_call("Space key filter set to: TESTKEY")

    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('builtins.input')
    @patch('builtins.print')
    def test_menu_option_set_max_results_filter(self, mock_print, mock_input, _mock_whoosh_available): # Adjusted args
        '''Test selecting option '2' to set the max results per space filter.'''
        mock_input.side_effect = ['2', '100', 'q']

        try:
            explore_clusters.main()
        except SystemExit:
            pass

        mock_input.assert_any_call("\nEnter your choice (or 'h' for help, 'q' to quit): ")
        mock_input.assert_any_call("Enter max results per space (integer), or leave blank for no limit: ")
        mock_print.assert_any_call("Max results per space filter set to: 100")

    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('builtins.input')
    @patch('explore_clusters.search_applications_indexed')
    @patch('builtins.print')
    def test_menu_option_10_search_indexed(self, mock_print, mock_search_applications_indexed, mock_input, _mock_whoosh_available): # Adjusted args
        '''Test selecting option '10' for search_applications_indexed.'''
        mock_input.side_effect = ['10', 'q']

        try:
            explore_clusters.main()
        except SystemExit:
            pass

        mock_search_applications_indexed.assert_called_once()
        mock_input.assert_any_call("\nEnter your choice (or 'h' for help, 'q' to quit): ")

    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('builtins.input')
    @patch('explore_clusters.search_applications_indexed_top_space_per_term')
    @patch('builtins.print')
    def test_menu_option_20_search_top_space_per_term(self, mock_print, mock_search_top_space_per_term, mock_input, _mock_whoosh_available): # Adjusted args
        '''Test selecting option '20' for search_applications_indexed_top_space_per_term.'''
        mock_input.side_effect = ['20', 'q']

        try:
            explore_clusters.main()
        except SystemExit:
            pass

        mock_search_top_space_per_term.assert_called_once()
        mock_input.assert_any_call("\nEnter your choice (or 'h' for help, 'q' to quit): ")

    @patch('explore_clusters.WHOOSH_AVAILABLE', True)
    @patch('builtins.input')
    @patch('explore_clusters.search_content_and_get_application_context')
    @patch('builtins.print')
    def test_menu_option_21_search_content_context(self, mock_print, mock_search_content_context, mock_input, _mock_whoosh_available): # Adjusted args
        '''Test selecting option '21' for search_content_and_get_application_context.'''
        mock_input.side_effect = ['21', 'q']

        try:
            explore_clusters.main()
        except SystemExit:
            pass

        mock_search_content_context.assert_called_once()
        mock_input.assert_any_call("\nEnter your choice (or 'h' for help, 'q' to quit): ")

if __name__ == '__main__':
    unittest.main()
