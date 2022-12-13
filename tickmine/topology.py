class Topology:
    '''
    port分配规则:
    tsaodai1_* 812* 8120 是debug口 8219是nginx口
    citic1_* 811* 8110 是debug口 8119是nginx口
    citic2_* 814* 8140 是debug口 8149是nginx口
    zhongtai1_* 813* 8130 是debug口 8139是nginx口
    '''

    def __init__(self, _local=True):
        self.gradation_list = []
        self.ip_dict = {}
        self.is_local = _local

        gradation = {
            'docker_name': 'tickserver_tsaodai1_1',
            'access_api': 'tcp://192.168.0.102:8120',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_tsaodai1_2',
            'access_api': 'tcp://192.168.0.102:8121',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_citic1_1',
            'access_api': 'tcp://192.168.0.102:8110',
            'contain_year': ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_citic1_2-4',
            'access_api': 'tcp://192.168.0.102:8119',
            'contain_year': ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_citic2_1',
            'access_api': 'tcp://192.168.0.102:8140',
            'contain_year': ['2023', '2024', '2025', '2026', '2027', '2028'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_citic2_2',
            'access_api': 'tcp://192.168.0.102:8141',
            'contain_year': ['2023', '2024', '2025', '2026', '2027', '2028'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_zhongtai1_1',
            'access_api': 'tcp://192.168.0.102:8130',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['SHSE', 'SZSE'],
            'contain_type': ['security'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_zhongtai1_2',
            'access_api': 'tcp://192.168.0.102:8131',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['SHSE', 'SZSE'],
            'contain_type': ['security'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())

        for item in self.gradation_list:
            if self.is_local == True:
                self.ip_dict[item['docker_name']] = item['access_api']
            else:
                self.ip_dict[item['docker_name']] = item['access_api'].replace('192.168.0.102', 'onepiece.cdsslh.com')


topology = Topology()
