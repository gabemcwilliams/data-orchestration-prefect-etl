## !!! THIS IS PART OF THE CI-CD PIPELINE.  DO NOT ALTER OR DELETE !!!
# written: gmcwilliams

from prefect.client.orchestration import get_client
import os
import re
import subprocess
import traceback
import yaml
import asyncio
from pathlib import Path


class DeployFlows:
    def __init__(self, src_dir: str = "./", api_uri = 'https://prefect.data-automation.internal:4200') -> None:
        self.src_dir = src_dir
        self.api_uri = api_uri

        # track issue trace
        self.issues_list = []

        # track results of script
        self.results_dict = {
            "success": {
                "linux": 0,
                "windows": 0,
            },
            "failure": 0,
            "skipped": 0,
        }

        # pull yaml information from job deployment files
        self.__deployment_info = self.__pull_deployment_info()

        # compare collected info with prefect deployment return results and deploy missing
        asyncio.run(self.check_and_deploy())

        # return results of script
        self.print_results()

    def __pull_deployment_info(self, flow_info_list: list = []) -> list:
        for root, _, files in os.walk(r"./"):
            for file in files:
                if file.endswith("-deploy.yaml"):
                    file_path = re.sub(r"\\+", "/", f'{root}/{file}')
                    result = re.match(r'(.*)-deploy.yaml', file)
                    if result:
                        flow_name = result.group(1)
                        root_path = re.sub(r"\\", "/", str(Path(__file__).parent.resolve()))
                        rel_path = root_path + file_path[1:]
                        # Read YAML file
                        with open(file_path, 'r') as stream:
                            try:
                                info_dict = dict()
                                data = yaml.safe_load(stream)
                                info_dict['product_source'] = data['name']
                                info_dict['version'] = data['prefect-version']

                                # deployments[0].*
                                _deployments = data.get('deployments', [])[0]
                                info_dict['name'] = _deployments.get('name')  # str
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
        # init issues list
        issues_list = []

        # iterate through each deployment info
        async with get_client() as client:
            deployments = [response.name for response in await client.read_deployments()]
            for info in self.__deployment_info:

                # Check if the deployment exists and is active
                if info['name'] in deployments:
                    print(f"\033[32mDeployment {info['name']} is [ACTIVE], skipping!\033[0m")
                    self.results_dict['skipped'] += 1

                else:
                    try:
                        print(
                            f"\033[33mDeployment {info['name']} [DOES NOT EXIST], proceed with deployment!\033[0m")
                        try:
                            print(f'yes n | prefect deploy --prefect-file {info["rel_path"]} --all')
                            subprocess.run(
                                f'yes n | prefect deploy --prefect-file {info["rel_path"]} --all',
                                shell=True,
                                check=True
                            )

                            self.results_dict['success']['linux'] = \
                                self.results_dict['success'].get('linux', 0) + 1

                        except:
                            print(f'echo y | prefect deploy --prefect-file {info["rel_path"]} --all')
                            subprocess.run(
                                f'echo y | prefect deploy --prefect-file {info["rel_path"]} --all',
                                shell=True,
                                check=True
                            )
                            self.results_dict['success']['windows'] = \
                                self.results_dict['success'].get('windows', 0) + 1

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

