class Topology:
    '''
    port分配规则:
    citic1_* 911* 9110 是debug口 8911是nginx口
    citic_self1_* 912* 9120 是debug口 8912是nginx口
    zhongtai1_* 913* 9130 是debug口 8913是nginx口
    citic2_* 914* 9140 是debug口 8914是nginx口
    summary1_* 915* 9150 是debug口 8915是nginx口
    baostock1_* 916* 9160 是debug口 8916是nginx口
    gate_self1_* 917* 9170 是debug口 8917是nginx口
    gate1_* 918* 9180 是debug口 8918是nginx口
    fxcm_self1_* 919* 9190 是debug口 8919是nginx口
    fxcm1_* 920* 9200 是debug口 8920是nginx口
    '''

    def __init__(self):
        self.gradation_list = []
        self.ip_dict = {}

        gradation = {
            'docker_name': 'tickserver_citic1',
            'access_api': '192.168.0.104:8911',
            'contain_year': ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_citic_self1',
            'access_api': '192.168.0.104:8912',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_zhongtai1',
            'access_api': '192.168.0.104:8913',
            'contain_year': ['2022', '2023', '2024', '2025', '2026', '2027'],
            'contain_exch': ['SHSE', 'SZSE'],
            'contain_type': ['security'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_citic2',
            'access_api': '192.168.0.104:8914',
            'contain_year': ['2023', '2024', '2025', '2026', '2027', '2028'],
            'contain_exch': ['CFFEX', 'CZCE', 'DCE', 'INE', 'SHFE'],
            'contain_type': ['future', 'option'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_summary1',
            'access_api': '192.168.0.104:8915',
            'contain_year': ['all'],
            'contain_exch': ['all'],
            'contain_type': ['all'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_baostock1',
            'access_api': '192.168.0.104:8916',
            'contain_year': ['all'],
            'contain_exch': ['all'],
            'contain_type': ['all'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_gate_self1',
            'access_api': '192.168.0.104:8917',
            'contain_year': ['2024', '2025'],
            'contain_exch': ['GATE'],
            'contain_type': ['crypto'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_gate1',
            'access_api': '192.168.0.104:8918',
            'contain_year': ['2024', '2025'],
            'contain_exch': ['GATE'],
            'contain_type': ['crypto'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_fxcm_self1',
            'access_api': '192.168.0.104:8919',
            'contain_year': ['2024', '2025'],
            'contain_exch': ['FXCM'],
            'contain_type': ['forex'],
            'mode': 'debug'
        }
        self.gradation_list.append(gradation.copy())

        gradation = {
            'docker_name': 'tickserver_fxcm1',
            'access_api': '192.168.0.104:8920',
            'contain_year': ['2024', '2025'],
            'contain_exch': ['FXCM'],
            'contain_type': ['forex'],
            'mode': 'release'
        }
        self.gradation_list.append(gradation.copy())

        for item in self.gradation_list:
            self.ip_dict[item['docker_name']] = item['access_api']


topology = Topology()
