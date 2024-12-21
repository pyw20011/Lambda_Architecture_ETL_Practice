'''
Created on 2024. 12. 21.

@author: pyw20
'''

from com.aaa.etl.fred_hdfs import Fred2Hdfs

title_earnings_list = [
    'Average Hourly Earnings of All Employees: Construction in ',
    'Average Hourly Earnings of All Employees: Financial Activities in ',
    'Average Hourly Earnings of All Employees: Goods Producing in ',
    'Average Hourly Earnings of All Employees: Leisure and Hospitality in ',
    'Average Hourly Earnings of All Employees: Manufacturing in ',
    'Average Hourly Earnings of All Employees: Private Service Providing in ',
    'Average Hourly Earnings of All Employees: Professional and Business Services in ',
    'Average Hourly Earnings of All Employees: Trade, Transportation, and Utilities in '
]
title_unemployee = 'Unemployment Rate in '
title_household_income = 'Real Median Household Income in '
title_poverty = 'Estimated Percent of People of All Ages in Poverty for '
title_real_gdp = 'Real Gross Domestic Product: All Industry Total in '
title_tax_exemption = 'Total Tax Exemptions for '
title_labor_force = 'Civilian Labor Force in '

def get_filename(str_title):
    iStart = str_title.index(':') + 2
    iEnd = str_title.index(' in')
    title = str_title[iStart:iEnd].replace(' ', '_').replace(',', '')

    return 'earnings_' + title + '.csv'

if __name__ == '__main__':
    fred = Fred2Hdfs()
    
    fred.clear_input_files('outputs', 'unemployee_annual.csv')
    df_list = fred.getListFredDF('A', title_unemployee)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('unemployee_annual.csv', df)
        else:
            fred.appendCsv2Hdfs('unemployee_annual.csv', df)

        df.to_csv('outputs/unemployee_annual.csv', mode='a', index_label='date', header=(i==0))

    print('=========================Unemployee Annual Done!!')

    fred.clear_input_files('outputs', 'household_income.csv')
    df_list = fred.getListFredDF('A', title_household_income)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('household_income.csv', df)
        else:
            fred.appendCsv2Hdfs('household_income.csv', df)

        df.to_csv('outputs/household_income.csv', mode='a', index_label='date', header=(i==0))

    print('=========================Real Median Household Income Done!!')

    fred.clear_input_files('outputs', 'tax_exemption.csv')
    df_list = fred.getListFredDF('A', title_tax_exemption)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('tax_exemption.csv', df)
        else:
            fred.appendCsv2Hdfs('tax_exemption.csv', df)

        df.to_csv('outputs/tax_exemption.csv', mode='a', index_label='date', header=(i==0))

    print('=========================Total Tax Exemptions Done!!')

    fred.clear_input_files('outputs', 'civilian_force.csv')
    df_list = fred.getListFredDF('A', title_labor_force)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('civilian_force.csv', df)
        else:
            fred.appendCsv2Hdfs('civilian_force.csv', df)

        df.to_csv('outputs/civilian_force.csv', mode='a', index_label='date', header=(i==0))
    
    print('========================= Civilian Labor Force Done !!!')

    fred.clear_input_files('outputs', 'poverty.csv')
    df_list = fred.getListFredDF('A', title_poverty)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('poverty.csv', df)
        else:
            fred.appendCsv2Hdfs('poverty.csv', df)

        df.to_csv('outputs/poverty.csv', mode='a', index_label='date', header=(i==0))

    print('========================= Poverty Done!!')

    fred.clear_input_files('outputs', 'real_gdp.csv')
    df_list = fred.getListFredDF('A', title_real_gdp)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('real_gdp.csv', df)
        else:
            fred.appendCsv2Hdfs('real_gdp.csv', df)

        df.to_csv('outputs/real_gdp.csv', mode='a', index_label='date', header=(i==0))

    print('========================= Real GDP Done!!')

    fred.clear_input_files('outputs', 'unemployee_monthly.csv')
    df_list = fred.getListFredDF('M', title_unemployee)

    for i, df in enumerate(df_list):
        if i == 0:
            fred.writeCsv2Hdfs('unemployee_monthly.csv', df)
        else:
            fred.appendCsv2Hdfs('unemployee_monthly.csv', df)

        df.to_csv('outputs/unemployee_monthly.csv', mode='a', index_label='date', header=(i==0))

    print('========================= Unemployee Monthly Done!!')

    for i in range(len(title_earnings_list)):
        fred.clear_input_files('outputs', get_filename(title_earnings_list[i]))

    df_list_of_list = list(map(lambda title: fred.getListFredDF('M', title),title_earnings_list))

    for i, df_list in enumerate(df_list_of_list):
        for j, df in enumerate(df_list):

            if j == 0:
                filename = get_filename(df.title.values[0])
    
            if j == 0:
                fred.writeCsv2Hdfs(filename, df)
            else:
                fred.appendCsv2Hdfs(filename, df)
    
            filepath = 'outputs/' + filename
            df.to_csv(filepath, mode='a', index_label='date', header= (j == 0))

    print('========================= earnings Done!!')

    print('Done... ===================================================')