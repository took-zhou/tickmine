class Topology:
    '''
    port分配规则:
    192.168.0.102
    tsaodai1_* 812* 8120 是debug口 8219是nginx口
    citic1_* 811* 8110 是debug口 8119是nginx口
    citic2_* 814* 8140 是debug口 8149是nginx口
    zhongtai1_* 813* 8130 是debug口 8139是nginx口
    summary1_* 815* 8150 是debug口 8159是nginx口
    baostock1_* 816* 8160 是debug口 8169是nginx口
    192.168.0.104
    gate_self1_* 817* 8170 是debug口 8279是nginx口
    gate1_* 818* 8180 是debug口 8189是nginx口
    '''

    def __init__(self):
        self.gradation_list = []
        self.ip_dict = {}

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
        gradation = {
            'docker_name': 'tickserver_gate_self1_1',
            'access_api': 'tcp://192.168.0.104:8170',
            'contain_year': ['2024'],
            'contain_exch': ['GATE'],
            'contain_type': ['crypto'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_gate_self1_2',
            'access_api': 'tcp://192.168.0.104:8171',
            'contain_year': ['2024'],
            'contain_exch': ['GATE'],
            'contain_type': ['crypto'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_gate1_1',
            'access_api': 'tcp://192.168.0.104:8180',
            'contain_year': ['2024'],
            'contain_exch': ['GATE'],
            'contain_type': ['crypto'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_gate1_2',
            'access_api': 'tcp://192.168.0.104:8181',
            'contain_year': ['2024'],
            'contain_exch': ['GATE'],
            'contain_type': ['crypto'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_summary1_1',
            'access_api': 'tcp://192.168.0.102:8150',
            'contain_year': ['all'],
            'contain_exch': ['all'],
            'contain_type': ['all'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())
        gradation = {
            'docker_name': 'tickserver_summary1_2',
            'access_api': 'tcp://192.168.0.102:8151',
            'contain_year': ['all'],
            'contain_exch': ['all'],
            'contain_type': ['all'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        for item in self.gradation_list:
            self.ip_dict[item['docker_name']] = item['access_api']


topology = Topology()
