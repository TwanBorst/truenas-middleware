import collections
import glob
import socket
import subprocess

from middlewared.service import Service


class MseriesNvdimmService(Service):

    class Config:
        private = True
        namespace = 'mseries.nvdimm'

    def run_ixnvdimm(self, nvmem_dev):
        # TODO: MODULE_HEALTH
        base = f'ixnvdimm -r {nvmem_dev}'
        cmds = [
            f'{base} SLOT0_FWREV',
            f'{base} SLOT1_FWREV',
            f'{base} FW_SLOT_INFO',
            f'{base} NVM_LIFETIME',
            f'{base} ES_LIFETIME',
            f'{base} SPECREV',
        ]
        return subprocess.run(
            ';'.join(cmds),
            stdout=subprocess.PIPE,
            shell=True,
            encoding="utf-8",
            errors="ignore",
        ).stdout.strip().split('\n')

    def parse_ixnvdimm_output(self, data):
        return {
            'running_firmware': '.'.join(data[0][:2] if data[2][-1] == '0' else data[1][:2]),
            'nvm_lifetime_percent': int(f'0x{data[3]}, 16'),
            'es_lifetime_percent': int(f'0x{data[4]}, 16'),
            'specrev': data[5],
        }

    def get_vendor_info(self, nvmem_dev):
        info = (
            'vendor', 'device', 'rev_id',
            'subsystem_vendor', 'subsystem_device', 'subsystem_rev_id',
            'serial',
        )
        vendor_info = collections.OrderedDict([(i, '') for i in info])
        for filename in info:
            try:
                with open(f'/sys/bus/nd/devices/{nvmem_dev}/nfit/{filename}') as f:
                    value = int(f.read().strip(), 16)
                    if filename == 'serial':
                        vendor_info[filename] = hex(socket.ntohl(value)).removeprefix('0x')
                    else:
                        vendor_info[filename] = hex(socket.ntohs(value))
            except (ValueError, FileNotFoundError):
                pass

        mapping = {
            '0x2c80_0x4e32_0x31_0x3480_0x4131_0x1': {
                'part_num': '18ASF2G72PF12G6V21AB',
                'size': '16GB', 'clock_speed': '2666MHz',
                'qualified_firmare': ['2.1', '2.2', '2.4'],
            },
            '0x2c80_0x4e36_0x31_0x3480_0x4231_0x2': {
                'part_num': '18ASF2G72PF12G9WP1AB',
                'size': '16GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['2.2'],
            },
            '0x2c80_0x4e33_0x31_0x3480_0x4231_0x1': {
                'part_num': '36ASS4G72PF12G9PR1AB',
                'size': '32GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['2.4'],
            },
            '0xc180_0x4e88_0x33_0xc180_0x4331_0x1': {
                'part_num': 'AGIGA8811-016ACA',
                'size': '16GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['0.8'],
            },
            '0xce01_0x4e39_0x34_0xc180_0x4331_0x1': {
                'part_num': 'AGIGA8811-032ACA',
                'size': '32GB', 'clock_speed': '2933MHz',
                'qualified_firmare': ['0.8'],
            },
            'unknown': {
                'part_num': None,
                'size': None, 'clock_speed': None,
                'qualified_firmware': [],
            }
        }
        key = '_'.join([v for k, v in vendor_info.items() if k != 'serial'])
        vendor_info.update(mapping.get(key, mapping['unknown']))
        return vendor_info

    def info(self):
        results = []
        sys = ("TRUENAS-M40", "TRUENAS-M50", "TRUENAS-M60")
        if not self.middleware.call_sync("truenas.get_chassis_hardware").startswith(sys):
            return results

        try:
            for nmem in glob.glob("/dev/nmem*"):
                info = {'dev': nmem.removeprefix('/dev/'), 'dev_path': nmem}
                info.update(self.get_vendor_info(info['dev']))
                info.update(self.parse_ixnvdimm_output(self.run_ixnvdimm(nmem)))
                results.append(info)
        except Exception:
            self.logger.error("Unhandled exception obtaining nvdimm info", exc_info=True)
        else:
            return results
