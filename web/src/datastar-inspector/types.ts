/**
 * Type definitions for Datastar Inspector
 */

export type ViewMode = "json" | "table";

export interface InspectorState {
  expanded: boolean;
  filter: string;
  viewMode: ViewMode;
  panelWidth?: number;
  panelHeight?: number;
}

export type SignalValue = string | number | boolean | null | SignalValue[] | SignalObject;

export interface SignalObject {
  [key: string]: SignalValue;
}
