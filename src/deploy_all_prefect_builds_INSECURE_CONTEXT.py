## !!! THIS IS PART OF THE CI-CD PIPELINE.  DO NOT ALTER OR DELETE !!!
# written: gmcwilliams

import os
import ssl
import re
import subprocess
import traceback
import yaml
import asyncio
import argparse
from pathlib import Path
from prefect.client.orchestration import get_client

parser = argparse.ArgumentParser(description="Deploy Prefect flows")
parser.add_argument("--insecure", action="store_true", help="Temporarily disable SSL verification")
args = parser.parse_args()

if args.insecure:
    # Disable SSL verification completely
    os.environ["PYTHONWARNINGS"] = "ignore:Unverified HTTPS request"
    os.environ["REQUESTS_CA_BUNDLE"] = ""
    os.environ["SSL_CERT_FILE"] = ""
    ssl._create_default_https_context = ssl._create_unverified_context

print("🔐 SSL context is:", "DISABLED" if args.insecure else ssl.get_default_verify_paths().cafile)

class DeployFlows:
    def __init__(self, src_dir: str = "./") -> None:
        self.src_dir = src_dir
        self.issues_list = []
        self.results_dict = {
            "success": {"linux": 0, "windows": 0},
            "failure": 0,
            "skipped": 0,
        }
        self.__deployment_info = self.__pull_deployment_info()
        asyncio.run(self.check_and_deploy())
        self.print_results()

    def __pull_deployment_info(self, flow_info_list: list = []) -> list:
        for root, _, files in os.walk(self.src_dir):
            for file in files:
                if file.endswith("-deploy.yaml"):
                    file_path = re.sub(r"\\+", "/", f'{root}/{file}')
                    result = re.match(r'(.*)-deploy.yaml', file)
                    if result:
                        flow_name = result.group(1)
                        root_path = re.sub(r"\\", "/", str(Path(__file__).parent.resolve()))
                        rel_path = root_path + file_path[1:]
                        try:
                            with open(file_path, 'r') as stream:
                                info_dict = dict()
                                data = yaml.safe_load(stream)
                                info_dict['product_source'] = data['name']
                                info_dict['version'] = data['prefect-version']
                                _deployments = data.get('deployments', [])[0]
                                info_dict['name'] = _deployments.get('name')
                                info_dict['flow_name'] = flow_name
                                info_dict['rel_path'] = rel_path
                                flow_info_list.append(info_dict)
                        except Exception as e:
                            t = traceback.format_exc()
                            self.issues_list.append({
                                'file': file,
                                'error': f"\033[31m{t}\033[0m"
                            })
                            continue
        return flow_info_list

    async def check_and_deploy(self):
        print("\n=== Deploying Flows ===\n")

        async with get_client() as client:
            deployments = [response.name for response in await client.read_deployments()]
            for info in self.__deployment_info:
                if info['name'] in deployments:
                    print(f"\033[32mDeployment {info['name']} is [ACTIVE], skipping!\033[0m")
                    self.results_dict['skipped'] += 1
                else:
                    try:
                        print(f"\033[33mDeployment {info['name']} [DOES NOT EXIST], proceeding...\033[0m")
                        try:
                            print(f'yes n | prefect deploy --prefect-file {info["rel_path"]} --all')
                            subprocess.run(
                                f'yes n | prefect deploy --prefect-file {info["rel_path"]} --all',
                                shell=True,
                                check=True
                            )
                            self.results_dict['success']['linux'] += 1
                        except:
                            print(f'echo y | prefect deploy --prefect-file {info["rel_path"]} --all')
                            subprocess.run(
                                f'echo y | prefect deploy --prefect-file {info["rel_path"]} --all',
                                shell=True,
                                check=True
                            )
                            self.results_dict['success']['windows'] += 1
                    except Exception as e:
                        t = traceback.format_exc()
                        self.results_dict['failure'] += 1
                        self.issues_list.append({
                            'file': info['name'],
                            'error': f"\033[31m{t}\033[0m"
                        })

    def print_results(self):
        print("\n=== Deployment Results ===")
        print(f"Successful deployments:")
        print(f" - Linux: {self.results_dict['success']['linux']}")
        print(f" - Windows: {self.results_dict['success']['windows']}")
        print(f"Failed deployments: {self.results_dict['failure']}")
        print(f"Skipped deployments: {self.results_dict['skipped']}")

        print("\n=== Issues ===")
        for issue in self.issues_list:
            print(f" - File: {issue['file']}, Error: {issue['error']}")


if __name__ == "__main__":
    deploy_flows = DeployFlows()
