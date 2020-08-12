import unittest
import pandas as pd 
import numpy as np 
import Entrepidus_generator

class TestEntrepidus(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        df_sales_columns = ['Country', 'Diageo Customer ID', 'Diageo Customer Name', 
            'Invoice number', 'Type of Invoice',	'Invoice Date', 'Store code', 'Product Code', 
            'Quantity', 'Unit of measure', 'Total Amount WITHOUT TAX', 'Total Amount WITH TAX', 
            'Currency Code', 'Sales Representative Code']
        cls.df_sales = pd.DataFrame(columns=df_sales_columns)

        df_pebac_product_reference_columns = ['Dist_Code', 'Distributor', 'Product_store_id', 'Country', 'Diageo_Sku',
            'Relevant', 'Scale']
        cls.df_pebac_product_reference = pd.DataFrame(columns=df_pebac_product_reference_columns)


    def test_getting_user_input(self):

        root_path = 'C:/Users/BACELKEN/Documents/Automation'
        catalogs_root_path = '../../../Catalogs/Traditional_STR/'
        product_by_distributor_file_name = 'pebac_ref_prod.xlsx'
        STR_indicator = False
        country = 'Argentina'

        sales_file_path = root_path + '/sales.txt'
        store_txt_file_path = root_path + '/store.txt'
        pebac_master_data_product_file_path = catalogs_root_path + 'Product_catalog/' + product_by_distributor_file_name
        product_master_path = catalogs_root_path + 'Product_catalog/product_master.xlsx'
        customer_catalog_file_path = catalogs_root_path + 'Customer_catalog/' + country + '_customer_catalog.xlsx'
        dist_names_file_path = catalogs_root_path + 'dist_names.xlsx'
        customer_filling_reference_file_path = catalogs_root_path + 'Customer_catalog/z_customer_reference.xlsx'

        entrepidus_stock_directory_path = '/'.join(root_path.split('/')[:-1])
        entrepidus_stock_file_path = entrepidus_stock_directory_path + '/Entrepidus_STOCK.csv'

        expected_paths = [sales_file_path, pebac_master_data_product_file_path, 
            product_master_path, customer_catalog_file_path, dist_names_file_path, root_path,
            entrepidus_stock_file_path, store_txt_file_path, customer_filling_reference_file_path]

        
        self.assertEqual(Entrepidus_generator.getting_system_paths(root_path, 
            country, STR_indicator), expected_paths)    


    def test_sanitizing_sales_file(self):
        
        self.df_sales['Quantity'] = ['55-', 37]
        self.df_sales['Total Amount WITH TAX'] = ['88-','']
        self.df_sales['Total Amount WITHOUT TAX'] = [77, '56.7-']
        self.df_sales.fillna('', inplace=True)

        df_expected = pd.DataFrame(columns=self.df_sales.columns)

        df_expected['Quantity'] = [-55, 37]
        df_expected['Total Amount WITH TAX'] = [-88.0, 0]
        df_expected['Total Amount WITHOUT TAX'] = [77, -56.7]
        df_expected.fillna('', inplace=True)

        pd.testing.assert_frame_equal(Entrepidus_generator.sanitizing_sales_file(self.df_sales), df_expected)


    def test_sanitizing_df_pebac_product_reference(self):

        self.df_pebac_product_reference['Scale'] = [6, np.nan]  

        df_expected = pd.DataFrame(columns=self.df_pebac_product_reference.columns)
        df_expected['Scale'] = [6.0, 1.0]
        
        pd.testing.assert_frame_equal(Entrepidus_generator.sanitizing_df_pebac_product_reference(self.df_pebac_product_reference), df_expected)



if __name__ == '__main__':
    unittest.main()