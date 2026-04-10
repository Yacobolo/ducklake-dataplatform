-- +goose Up
ALTER TABLE dashboard_widgets ADD COLUMN page_name TEXT NOT NULL DEFAULT 'Overview';

UPDATE dashboard_widgets
SET page_name = 'Overview'
WHERE TRIM(COALESCE(page_name, '')) = '';

-- +goose Down
