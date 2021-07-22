/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"github.com/vmware/govmomi/object"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpod "k8s.io/kubernetes/test/e2e/framework/pod"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"
	"sigs.k8s.io/vsphere-csi-driver/pkg/apis/migration/v1alpha1"
)

var _ = ginkgo.Describe("[csi-vcp-mig] VCP to CSI migration attach, detach tests", func() {
	f := framework.NewDefaultFramework("vcp-2-csi-attach-detach")
	var (
		client                     clientset.Interface
		namespace                  string
		nodeList                   *v1.NodeList
		vcpScs                     []*storagev1.StorageClass
		vcpPvcsPreMig              []*v1.PersistentVolumeClaim
		vcpPvsPreMig               []*v1.PersistentVolume
		vcpPvcsPostMig             []*v1.PersistentVolumeClaim
		vcpPvsPostMig              []*v1.PersistentVolume
		err                        error
		kcmMigEnabled              bool
		kubectlMigEnabled          bool
		isSPSserviceStopped        bool
		isVsanHealthServiceStopped bool
		vmdks                      []string
		pvsToDelete                []*v1.PersistentVolume
		fullSyncWaitTime           int
		podsToDelete               []*v1.Pod
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = f.Namespace.Name
		bootstrap()
		nodeList, err = fnodes.GetReadySchedulableNodes(f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		generateNodeMap(ctx, testConfig, &e2eVSphere, client)

		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
		kubectlMigEnabled = false

		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = false

		pvsToDelete = []*v1.PersistentVolume{}

		if os.Getenv(envFullSyncWaitTime) != "" {
			fullSyncWaitTime, err = strconv.Atoi(os.Getenv(envFullSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			// Full sync interval can be 1 min at minimum so full sync wait time has to be more than 120s
			if fullSyncWaitTime < 120 || fullSyncWaitTime > defaultFullSyncWaitTime {
				framework.Failf("The FullSync Wait time %v is not set correctly", fullSyncWaitTime)
			}
		} else {
			fullSyncWaitTime = defaultFullSyncWaitTime
		}
	})

	ginkgo.JustAfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var pvcsToDelete []*v1.PersistentVolumeClaim
		connect(ctx, &e2eVSphere)
		if kcmMigEnabled {
			pvcsToDelete = append(vcpPvcsPreMig, vcpPvcsPostMig...)
		} else {
			pvcsToDelete = append(pvcsToDelete, vcpPvcsPreMig...)
		}
		vcpPvcsPreMig = []*v1.PersistentVolumeClaim{}
		vcpPvcsPostMig = []*v1.PersistentVolumeClaim{}

		vcAddress := e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort

		if isVsanHealthServiceStopped {
			ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
			err = invokeVCenterServiceControl("start", vsanhealthServiceName, vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(
				fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to come up again", vsanHealthServiceWaitTime),
			)
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		if isSPSserviceStopped {
			ginkgo.By(fmt.Sprintln("Starting sps on the vCenter host"))
			err = invokeVCenterServiceControl("start", "sps", vcAddress)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow sps to come up again", vsanHealthServiceWaitTime))
			time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)
		}

		for _, pod := range podsToDelete {
			ginkgo.By(fmt.Sprintf("Deleting pod: %s", pod.Name))
			volhandles := []string{}
			for _, vol := range pod.Spec.Volumes {
				pv := getPvFromClaim(client, namespace, vol.PersistentVolumeClaim.ClaimName)
				volhandles = append(volhandles, getVolHandle4Pv(ctx, client, pv))
			}
			err = client.CoreV1().Pods(namespace).Delete(ctx, pod.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			for _, volHandle := range volhandles {
				ginkgo.By("Verify volume is detached from the node")
				isDiskDetached, err := e2eVSphere.waitForVolumeDetachedFromNode(client, volHandle, pod.Spec.NodeName)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				gomega.Expect(isDiskDetached).To(
					gomega.BeTrue(),
					fmt.Sprintf("Volume %q is not detached from the node %q", volHandle, pod.Spec.NodeName),
				)
			}
		}

		if kubectlMigEnabled {
			ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
			toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
		}

		crds := []*v1alpha1.CnsVSphereVolumeMigration{}
		for _, pvc := range pvcsToDelete {
			pv, err := client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vPath := pv.Spec.VsphereVolume.VolumePath
			if kcmMigEnabled {
				found, crd := getCnsVSphereVolumeMigrationCrd(ctx, vPath)
				if found {
					crds = append(crds, crd)
				}
			}
			pvsToDelete = append(pvsToDelete, pv)

			framework.Logf("Deleting PVC %v", pvc.Name)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		var defaultDatastore *object.Datastore
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		for _, pv := range pvsToDelete {
			if pv.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimRetain {
				err = client.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				if defaultDatastore == nil {
					defaultDatastore = getDefaultDatastore(ctx)
				}
				if pv.Spec.CSI != nil {
					err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
					err = e2eVSphere.deleteFCD(ctx, pv.Spec.CSI.VolumeHandle, defaultDatastore.Reference())
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				} else {
					if kcmMigEnabled {
						found, crd := getCnsVSphereVolumeMigrationCrd(ctx, pv.Spec.VsphereVolume.VolumePath)
						gomega.Expect(found).To(gomega.BeTrue())
						err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
						err = e2eVSphere.deleteFCD(ctx, crd.Spec.VolumeID, defaultDatastore.Reference())
						gomega.Expect(err).NotTo(gomega.HaveOccurred())
					}
					err = deleteVmdk(esxHost, pv.Spec.VsphereVolume.VolumePath)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}
			if pv.Spec.CSI != nil {
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(pv.Spec.CSI.VolumeHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			} else {
				err = waitForVmdkDeletion(ctx, pv.Spec.VsphereVolume.VolumePath)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}

		for _, crd := range crds {
			framework.Logf("Waiting for CnsVSphereVolumeMigration crd %v to be deleted", crd.Spec.VolumeID)
			err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		vcpPvsPreMig = nil
		vcpPvsPostMig = nil

		if kcmMigEnabled {
			err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		vmdksToDel := vmdks
		vmdks = nil
		for _, vmdk := range vmdksToDel {
			err = deleteVmdk(esxHost, vmdk)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		var scsToDelete []*storagev1.StorageClass
		scsToDelete = append(scsToDelete, vcpScs...)
		vcpScs = []*storagev1.StorageClass{}
		for _, vcpSc := range scsToDelete {
			err := client.StorageV1().StorageClasses().Delete(ctx, vcpSc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
	})

	/*
		Tests 1, 2, 3, 4, 7, 8, 13, 14, 15, 17, 18, 19, 20 from the TDS
	*/
	ginkgo.It("Attach detach combined test", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		esxHost := GetAndExpectStringEnvVar(envEsxHostIP)
		spbmPolicyName := GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)
		ginkgo.By("Creating VCP SC")
		scParams := make(map[string]string)
		scParams[vcpScParamDatastoreName] = GetAndExpectStringEnvVar(envSharedDatastoreName)
		vcpSc, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc)

		ginkgo.By("Creating VCP PVCs before migration")
		pvc1, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc1)

		pvc2, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc2)

		vmdk3, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk3)

		pv3 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk3), v1.PersistentVolumeReclaimDelete, nil)
		pv3.Spec.StorageClassName = vcpSc.Name
		_, err = client.CoreV1().PersistentVolumes().Create(ctx, pv3, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		pvc3 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc, nil, "")
		pvc3.Spec.StorageClassName = &vcpSc.Name
		pvc3.Spec.VolumeName = pv3.Name
		pvc3, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc3, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc3)

		vmdk4, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk4)

		pv4 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk4), v1.PersistentVolumeReclaimDelete, nil)
		pv4.Spec.StorageClassName = vcpSc.Name
		_, err = client.CoreV1().PersistentVolumes().Create(ctx, pv4, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		pvc4 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc, nil, "")
		pvc4.Spec.StorageClassName = &vcpSc.Name
		pvc4.Spec.VolumeName = pv4.Name
		pvc4, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc4, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc4)

		pvc7, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc7)

		pvc13, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ns, err := framework.CreateTestingNS(f.BaseName, client, nil)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvcs14 := make([]*v1.PersistentVolumeClaim, 5)
		for i := 0; i < 5; i++ {
			pvcs14[i], err = createPVC(client, ns.Name, nil, "", vcpSc, "")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		vmdk15, err := createVmdk(esxHost, "", "", "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = append(vmdks, vmdk15)

		pv15 := getVcpPersistentVolumeSpec(getCanonicalPath(vmdk15), v1.PersistentVolumeReclaimDelete, nil)
		pv15.Spec.StorageClassName = vcpSc.Name
		_, err = client.CoreV1().PersistentVolumes().Create(ctx, pv15, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vmdks = []string{}

		pvc15 := getVcpPersistentVolumeClaimSpec(namespace, "", vcpSc, nil, "")
		pvc15.Spec.StorageClassName = &vcpSc.Name
		pvc15.Spec.VolumeName = pv15.Name
		pvc15, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc15, metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc15)

		ginkgo.By("Creating VCP SC with SPBM policy")
		scParams2 := make(map[string]string)
		scParams2[vcpScParamPolicyName] = spbmPolicyName
		vcpSc2, err := createVcpStorageClass(client, scParams2, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpSc2)

		pvc17, err := createPVC(client, namespace, nil, "", vcpSc2, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc17)

		pvc18, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPreMig = append(vcpPvcsPreMig, pvc18)

		pvc19, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pvc20, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all claims created before migration to be in bound state")
		vcpPvsPreMig, err = fpv.WaitForPVClaimBoundPhase(
			client, append(append(vcpPvcsPreMig, pvc13), pvcs14...), framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating standalone pods using VCP PVCs before migration")
		_, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc2}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc4}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		pod13, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc13}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for i := 0; i < 5; i++ {
			_, err = createPod(client, ns.Name, nil, []*v1.PersistentVolumeClaim{pvcs14[i]}, false, execCommand)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		pod15, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc15}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc18}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc19}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		_, err = createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc20}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify CnsVSphereVolumeMigration crd is not created for pvc used in test13")
		vpath := getvSphereVolumePathFromClaim(ctx, client, namespace, pvc13.Name)
		_, err = waitForCnsVSphereVolumeMigrationCrd(ctx, vpath, pollTimeoutShort)
		gomega.Expect(err).To(gomega.HaveOccurred())

		ginkgo.By("Delete pod created for test13")
		err = fpod.DeletePodWithWait(client, pod13)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pvc created for test13")
		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc13.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitForVmdkDeletion(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enabling CSIMigration and CSIMigrationvSphere feature gates on kube-controller-manager")
		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, true)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = true

		ginkgo.By("Waiting for migration related annotations on PV/PVCs created before migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, "", append(vcpPvcsPreMig, pvcs14...), vcpPvsPreMig, true)

		ginkgo.By("Verify CnsVSphereVolumeMigration crds and CNS volume metadata on pvc created before migration")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, "", append(vcpPvcsPreMig, pvcs14...))

		ginkgo.By("Delete pod created for test15")
		err = fpod.DeletePodWithWait(client, pod15)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pvc created for test15")
		vpath = getvSphereVolumePathFromClaim(ctx, client, namespace, pvc15.Name)
		crd, err := waitForCnsVSphereVolumeMigrationCrd(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc15.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = e2eVSphere.waitForCNSVolumeToBeDeleted(crd.Spec.VolumeID)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitForCnsVSphereVolumeMigrationCrdToBeDeleted(ctx, crd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		err = waitForVmdkDeletion(ctx, vpath)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Enable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, true)
		kubectlMigEnabled = true

		ginkgo.By("Creating VCP SC post migration")
		vcpScPost, err := createVcpStorageClass(client, scParams, nil, "", "", false, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpScs = append(vcpScs, vcpScPost)

		ginkgo.By("Creating VCP PVCs post migration")
		pvc7post, err := createPVC(client, namespace, nil, "", vcpSc, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc7post)

		pvc8post, err := createPVC(client, namespace, nil, "", vcpScPost, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		vcpPvcsPostMig = append(vcpPvcsPostMig, pvc8post)

		ginkgo.By("Waiting for all claims created post migration to be in bound state")
		vcpPvsPostMig, err = fpv.WaitForPVClaimBoundPhase(client, vcpPvcsPostMig, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Verify annotations on PV/PVCs created post migration")
		waitForMigAnnotationsPvcPvLists(ctx, client, namespace, vcpPvcsPostMig, vcpPvsPostMig, false)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes created post migration along with their " +
			"respective CnsVSphereVolumeMigration CRDs",
		)
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(ctx, client, namespace, vcpPvcsPostMig)

		ginkgo.By("Creating standalone pods using VCP PVCs post migration")
		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc1}),
		)
		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc2}),
		)
		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc3}),
		)
		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc4}),
		)
		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(
				ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc7, pvc7post},
			),
		)
		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc8post}),
		)

		for i := 0; i < 5; i++ {
			_ = createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvcs14[i]})
		}

		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc17}),
		)

		pod18 := createPodWithMultipleVolsVerifyVolMounts(
			ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc18},
		)

		_ = createPodWithMultipleVolsVerifyVolMounts(
			ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc19},
		)

		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc20}),
		)

		ginkgo.By("Delete pod created for test18")
		deletePodAndWaitForVolsToDetach(ctx, client, namespace, pod18)

		ginkgo.By("Restart CSi driver")
		updateDeploymentReplicawithWait(client, 0, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		updateDeploymentReplicawithWait(client, 1, vSphereCSIControllerPodNamePrefix, csiSystemNamespace)

		podsToDelete = append(
			podsToDelete,
			createPodWithMultipleVolsVerifyVolMounts(ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc18}),
		)

		ginkgo.By("Wait and verify CNS entries for all CNS volumes")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(
			ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc1, pvc2},
		)

		ginkgo.By("Delete pods")
		for _, pod := range podsToDelete {
			deletePodAndWaitForVolsToDetach(ctx, client, namespace, pod)
		}
		podsToDelete = nil

		ginkgo.By("Wait and verify CNS entries for all CNS volumes")
		verifyCnsVolumeMetadataAndCnsVSphereVolumeMigrationCrdForPvcs(
			ctx, client, namespace, []*v1.PersistentVolumeClaim{pvc1, pvc2},
		)

		vmdkToWaitForDeletion := []string{}
		ginkgo.By("Delete namespace created for test14")
		for _, pvc := range pvcs14 {
			pv := getPvFromClaim(client, namespace, pvc.Name)
			vmdkToWaitForDeletion = append(vmdkToWaitForDeletion, pv.Spec.VsphereVolume.VolumePath)
		}
		err = client.CoreV1().Namespaces().Delete(ctx, ns.Name, *metav1.NewDeleteOptions(0))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Delete pvcs")
		for _, pvc := range append(vcpPvcsPreMig, vcpPvcsPostMig...) {
			pv := getPvFromClaim(client, namespace, pvc.Name)
			framework.Logf("Deleting PVC %v", pvc.Name)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			vmdkToWaitForDeletion = append(vmdkToWaitForDeletion, pv.Spec.VsphereVolume.VolumePath)
		}

		ginkgo.By("Wait for vmdks used by VCP PVCs to be deleted")
		for _, vmdk := range vmdkToWaitForDeletion {
			err = waitForVmdkDeletion(ctx, vmdk)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		vmdkToWaitForDeletion = []string{}
		vcpPvsPreMig = nil
		vcpPvsPostMig = nil

		ginkgo.By("Disable CSI migration feature gates on kublets on k8s nodes")
		toggleCSIMigrationFeatureGatesOnK8snodes(ctx, client, false)
		kubectlMigEnabled = false

		err = toggleCSIMigrationFeatureGatesOnKubeControllerManager(ctx, client, false)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		kcmMigEnabled = false

		ginkgo.By("Creating pvc post reset")
		pvc20reset, err := createPVC(client, namespace, nil, "", vcpScPost, "")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Waiting for all claims created post migration to be in bound state")
		_, err = fpv.WaitForPVClaimBoundPhase(
			client, []*v1.PersistentVolumeClaim{pvc20reset}, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Creating pods post reset")
		pod19, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc19}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod20, err := createPod(client, namespace, nil, []*v1.PersistentVolumeClaim{pvc20}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		pod20reset, err := createPod(
			client, namespace, nil, []*v1.PersistentVolumeClaim{pvc20reset}, false, execCommand)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("Deleting pods created post reset")
		for _, pod := range []*v1.Pod{pod19, pod20, pod20reset} {
			err = fpod.DeletePodWithWait(client, pod)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Delete VCP PVCs post reset")
		for _, pvc := range []*v1.PersistentVolumeClaim{pvc20reset, pvc19, pvc20} {
			pv := getPvFromClaim(client, namespace, pvc.Name)
			vmdkToWaitForDeletion = append(vmdkToWaitForDeletion, pv.Spec.VsphereVolume.VolumePath)
			err = client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvc.Name, *metav1.NewDeleteOptions(0))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}
		ginkgo.By("Wait for vmdks used by VCP PVCs to be deleted")
		for _, vmdk := range vmdkToWaitForDeletion {
			err = waitForVmdkDeletion(ctx, vmdk)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})
})
