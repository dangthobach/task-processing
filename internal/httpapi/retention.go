package httpapi

import (
	"net/http"

	"github.com/example/task-processing/internal/application/controlplane"
	"github.com/google/uuid"
)

func (a *API) createRetentionPolicy(w http.ResponseWriter,r *http.Request){
	if !requireRole(w,r,"admin","developer"){return};var req struct{ProjectID uuid.UUID `json:"project_id"`;ResourceType string `json:"resource_type"`;RetentionDays int `json:"retention_days"`};if !decode(w,r,&req){return};if !a.projectOK(r.Context(),principal(r),req.ProjectID){problem(w,r,http.StatusNotFound,"PROJECT_NOT_FOUND","Project not found",false);return};out,err:= (controlplane.RetentionPolicyService{Store:a.Store}).Create(r.Context(),controlplane.CreateRetentionPolicy{ProjectID:req.ProjectID,ResourceType:req.ResourceType,RetentionDays:req.RetentionDays});if err!=nil{problem(w,r,http.StatusBadRequest,"INVALID_RETENTION_POLICY",err.Error(),false);return};a.audit(r.Context(),principal(r),req.ProjectID,"retention_policy.create","retention_policy",out.ID,req);a.emit(r.Context(),req.ProjectID,"retention_policy.changed","retention_policy",out.ID,map[string]any{"id":out.ID,"version":out.Version});writeJSON(w,http.StatusCreated,map[string]any{"id":out.ID,"status":out.Status,"version":out.Version})
}
