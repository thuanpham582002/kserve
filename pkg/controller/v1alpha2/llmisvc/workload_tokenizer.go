/*
Copyright 2025 The KServe Authors.

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

package llmisvc

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kserve/kserve/pkg/apis/serving/v1alpha2"
	"github.com/kserve/kserve/pkg/constants"
	"github.com/kserve/kserve/pkg/credentials"
)

const (
	// TokenizerFetchContainerName is the name of the init container that fetches tokenizers.
	TokenizerFetchContainerName = "tokenizer-fetch"
	// TokenizerVolumeName is the name of the shared volume for tokenizer files.
	TokenizerVolumeName = "tokenizer-volume"
	// TokenizerMountPath is the mount path where tokenizers are stored.
	TokenizerMountPath = "/shared/tokenizer"
)

// attachTokenizerInit adds tokenizer-fetch init container to scheduler pod.
// Returns true if tokenizer was attached, false otherwise.
// Errors are logged but don't fail reconciliation - EPP can work without tokenizer.
func (r *LLMISVCReconciler) attachTokenizerInit(
	ctx context.Context,
	serviceAccount *corev1.ServiceAccount,
	llmSvc *v1alpha2.LLMInferenceService,
	podSpec *corev1.PodSpec,
	config *Config,
) (bool, error) {
	logger := log.FromContext(ctx)

	modelUri := llmSvc.Spec.Model.URI.String()

	// S3-only in this phase
	if !strings.HasPrefix(modelUri, constants.S3URIPrefix) {
		logger.V(2).Info("Skipping tokenizer init - S3 URI required", "uri", modelUri)
		return false, nil
	}

	// Check disable annotation
	if llmSvc.Annotations != nil {
		if _, ok := llmSvc.Annotations["serving.kserve.io/disable-tokenizer-init"]; ok {
			logger.V(2).Info("Skipping tokenizer init - disabled via annotation")
			return false, nil
		}
	}

	initContainer := &corev1.Container{
		Name:  TokenizerFetchContainerName,
		Image: config.TokenizerFetchImage,
		Args:  []string{"--source", modelUri, "--dest", TokenizerMountPath},
		VolumeMounts: []corev1.VolumeMount{
			{Name: TokenizerVolumeName, MountPath: TokenizerMountPath},
		},
	}

	// Attach credentials
	if err := r.attachTokenizerCredentials(ctx, serviceAccount, llmSvc,
		initContainer, podSpec, config); err != nil {
		return false, fmt.Errorf("attach tokenizer credentials: %w", err)
	}

	// Inject CA bundle (replicate pattern from attachS3ModelArtifact)
	if config.StorageConfig != nil {
		injectCaBundle(llmSvc.Namespace, podSpec, initContainer, config.StorageConfig)
	}

	// Add emptyDir volume
	addVolumeIfNotExists(podSpec, TokenizerVolumeName)
	podSpec.InitContainers = append(podSpec.InitContainers, *initContainer)

	// Mount to main container (read-only)
	mountToMainContainer(podSpec, TokenizerVolumeName, TokenizerMountPath)

	logger.Info("Attached tokenizer-fetch init container", "uri", modelUri)
	return true, nil
}

func (r *LLMISVCReconciler) attachTokenizerCredentials(
	ctx context.Context,
	serviceAccount *corev1.ServiceAccount,
	llmSvc *v1alpha2.LLMInferenceService,
	initContainer *corev1.Container,
	podSpec *corev1.PodSpec,
	config *Config,
) error {
	logger := log.FromContext(ctx)

	// Guard against nil credential config - continue without credentials (public bucket fallback)
	if config.CredentialConfig == nil {
		logger.V(2).Info("CredentialConfig is nil, continuing without credentials")
		return nil
	}

	// Use provided SA or fetch default
	if serviceAccount == nil {
		serviceAccount = &corev1.ServiceAccount{}
		if err := r.Get(ctx, types.NamespacedName{
			Name: "default", Namespace: llmSvc.Namespace,
		}, serviceAccount); err != nil {
			// Log but don't fail - credentials may not be required (public bucket)
			logger.V(2).Info("Default service account not found, continuing without credentials")
			return nil
		}
	}

	credBuilder := credentials.NewCredentialBuilderFromConfig(
		r.Client, r.Clientset, *config.CredentialConfig,
	)

	if err := credBuilder.CreateSecretVolumeAndEnvFromServiceAccount(
		ctx, serviceAccount, llmSvc.Annotations, initContainer, &podSpec.Volumes,
	); err != nil {
		return fmt.Errorf("create secret volume from service account: %w", err)
	}

	return nil
}

func addVolumeIfNotExists(podSpec *corev1.PodSpec, name string) {
	for _, v := range podSpec.Volumes {
		if v.Name == name {
			return
		}
	}
	podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
		Name:         name,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
}

func mountToMainContainer(podSpec *corev1.PodSpec, volName, mountPath string) {
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name != "main" {
			continue
		}
		for _, m := range podSpec.Containers[i].VolumeMounts {
			if m.Name == volName {
				return
			}
		}
		podSpec.Containers[i].VolumeMounts = append(
			podSpec.Containers[i].VolumeMounts,
			corev1.VolumeMount{Name: volName, MountPath: mountPath, ReadOnly: true},
		)
	}
}
