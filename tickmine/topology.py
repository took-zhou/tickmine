class Topology:

    def __init__(self):
        self.gradation_list = []

        gradation = {
            'docker_name': ['tickmine_tsaodai1_1'],
            'access_api': 'tcp://192.168.0.102:8120',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': ['tickmine_tsaodai1_2'],
            'access_api': 'tcp://192.168.0.102:8100',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': ['tickmine_citic1_1'],
            'access_api': 'tcp://192.168.0.102:8110',
            'contain_year': ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': ['tickmine_citic1_2', 'tickmine_citic1_3', 'tickmine_citic1_4'],
            'access_api': 'tcp://192.168.0.102:8101',
            'contain_year': ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': ['tickmine_citic2_1'],
            'access_api': 'tcp://192.168.0.102:8140',
            'contain_year': ['2023', '2024', '2025', '2026', '2027', '2028'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': ['tickmine_citic2_2', 'tickmine_citic2_3', 'tickmine_citic2_4'],
            'access_api': 'tcp://192.168.0.102:8151',
            'contain_year': ['2023', '2024', '2025', '2026', '2027', '2028'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': ['tickmine_zhongtai1_1'],
            'access_api': 'tcp://192.168.0.102:8130',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['SHSE', 'SZSE'],
            'contain_type': ['security'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': ['tickmine_zhongtai1_2'],
            'access_api': 'tcp://192.168.0.102:8150',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['SHSE', 'SZSE'],
            'contain_type': ['security'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())


topology = Topology()
