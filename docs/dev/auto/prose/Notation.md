# Structure Formula Notation
Structure Formula Notation is math formula-like notation for Structure transforms 
and their parts: inputs, outputs, stages and steps, as well as stage and step method
arguments and return types.
The goal is leverage LaTex formatting for compact display of Structure transforms. 

In this notation
- fields are displayed as vector components, 
- methods are displayed as math functions, 
- method arguments as function arguments,
- method arguments types as :Type annotation on function argument, 
- method return types as :Type annotation on function;
- transform's input/output sets as vertical vector (in parenthesis)
- transform's individual inputs/outputs as 'name : Type' pairs

## Style tips
- Include line heights (e.g. \\[12pt]) if needed to prevent overlapping. 
- Escape every underscore in displayed identifiers as `\\_`; formulas use no subscripts.

## Notation Variants
Notation comes as default notation and variants
- Default notation fully features all details such as method return types.
- 'omit_' variants omit one part of default notation 
  - Ex: 
    - omit_argument_names altogether omits arguments (including parentheses after function name)  
    - omit_argument_types omits function argument types 
    - omit_input_names omits type annotations (left part and colon from 'name : Type' pair) from transform input vector  
    - omit_input_types omits type annotations (right part of 'name : Type' pair) from transform input vector  
    - omit_names omits names in schema notation  
    - omit_types omits type annotations in schema notation  
- Combination variants 
  - Ex: Variant 'compact' is a combination of all omit_: omit_argument_names, omit_return_types, omit_input_output_names
- Bracing variants
  - use_parentheses - use pmatrix instead of bmatrix, Bmatrix
  - use_brackets - use bmatrix instead of pmatrix, Bmatrix
- Canonic variant
  - Canonic variant is the default variant for when a construct (transforms and their parts) is used standalone, 
  e.g. on a dedicated line/paragraph, as opposed to be shown as part of a bigger construct.  
  By default, it is 'default', but can be additionally specified per-section below. 

## Schema Notation
Structure notation for schema classes.

### Schema Notation - Default
Field name vector without type annotations

\begin{pmatrix}
x \\
y \\
z 
\end{pmatrix}

### Schema Notation - With Types
Field vector with type annotations

\begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}

### Schema Notation - With Name
Notation for defining a schema class.

\operatorname{SchemaClass} : \begin{pmatrix}
x \\
y \\
z
\end{pmatrix}

### Schema Notation - With Projection
Schema class created as projection of another class or classes.
Based on 'Schema Notation - With Name' but omits the projected fields. 
Used to describe step method return value obtained as a projection (.project() or .base()). 
In such case, only non-projected (new/unique) fields are shown.

\operatorname{SchemaClass} : \begin{pmatrix}
\vdots \\
z
\end{pmatrix}

### Schema Notation Variants
The variants are as described in 'Notation Variants':
- omit_names: with_types, omit names
- omit_types
- with_name: defined in 'Schema Notation - With Name' above
- with_projection: defined in 'Schema Notation - With Projection' above
- default: omit_types 
- compact: default
- canonic: default

## Step Method Notation
Use single-argument/multi-argument notation depending on the number of arguments.

### Step Method Notation - Single-Argument
Notation for single-argument step methods
\operatorname{func}(x: X) \rightarrow A

### Step Method Notation - Horizontal
\operatorname{func}(x: X, z : Z) \rightarrow A

### Step Method Notation - Vertical
Notation for a multi-argument methods
\operatorname{func3}\!\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B

### Step Method Notation - Multiple Return Values
Notation for a multiple return value methods. Uses a vector to show return types.
Ex (for vertical notation):
\operatorname{func3}\!\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow \begin{pmatrix} B \\ C \end{pmatrix}

### Step Method Notation - Colon
Fancy colon (' : ') after operator. 
Ex:
\operatorname{func3} : \begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B

### Step Method Notation - Return Schema Definitions
Use with_projection variant of schema notation for the schemas of return types. 
Ex:
\operatorname{func3}\!\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B : \begin{pmatrix} c \\ d \end{pmatrix} 

### Step Method Notation Variants
The variants are as described in 'Notation Variants' section:
- omit_argument_names
- omit_argument_types
- omit_return_types
- single_argument
- vertical
- multiple_return_values
- return_schema_definitions: 'Step Method Notation - Return Schema Definitions'
- compact
- default: (single_argument or vertical) + multiple_return_values
- canonic: default + omit_argument_names + return_schema_definitions

The canonic step-method form omits argument names and the separating colons, but retains each argument's schema
type. Its return schema retains the schema name and field names while omitting field type annotations.

## Step Transform Notation
Notation for a transform with step methods. 

### Step Transform Notation - Default:
Combines inputs vector, step methods vector and outputs vector.

\begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}
\odot
\begin{Bmatrix}
\operatorname{func1}(x : X) \rightarrow D \\
\operatorname{func2}(y : Y) \rightarrow A \\
\operatorname{func3}\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B
\end{Bmatrix}
\rightarrow
\begin{pmatrix}
a : A \\
b : B \\
c : C
\end{pmatrix}

### Step Transform Notation - With Name
Notation for defining a transform.
Based on default notation, preceded with transform name.

\operatorname{TransformClassName} : \begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}
\begin{Bmatrix}
\operatorname{func1}(x : X) \rightarrow D \\
\operatorname{func2}(y : Y) \rightarrow A \\
\operatorname{func3}\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B
\end{Bmatrix}
\rightarrow
\begin{pmatrix}
a : A \\
b : B \\
c : C
\end{pmatrix}

### Step Transform Notation - As a Call
Notation for displaying a transform as a stage of a bigger transform. 
Based on 'With Name' notation, without colon and smaller space to inputs vector.

\operatorname{TransformClassName}\!\begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}
\begin{Bmatrix}
\operatorname{func1}(x : X) \rightarrow D \\
\operatorname{func2}(y : Y) \rightarrow A \\
\operatorname{func3}\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B
\end{Bmatrix}
\rightarrow
\begin{pmatrix}
a : A \\
b : B \\
c : C
\end{pmatrix}

### Step Transform Notation - Variants
The variants are as described in 'Notation Variants' and above sections:
- omit_input_names
- omit_output_names
- omit_input_output_names
- omit_argument_names
- omit_argument_types
- omit_return_types
- omit_odot: omit \odot sign
- with_name: 'Step Transform Notation - With Name'
- as_call: 'Step Transform Notation - As a Call'
- compact
- canonic: with_name, omit_input_output_names, omit_argument_names, omit_return_types, omit_odot   

## Stage Notation
Notation for a transform which serves as a stage in a bigger transform.
Notation is used for both for standalone display of a stage as well as displaying as part of the enclosing transform.

### Stage Notation - Default:
Stage name followed by a colon, followed by inputs vector, arrow, followed by outputs vector.

\operatorname{Stage} : \begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}
\rightarrow
\begin{pmatrix}
a : A \\
b : B \\
c : C
\end{pmatrix}

### Stage Notation - With Steps:
Combines Stage Notation with step methods vector from Step Transform Notation.
Same as Step Transform Notation with stage name on the left.

\operatorname{Stage} : \begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}
\begin{Bmatrix}
\operatorname{func1}(x : X) \rightarrow D \\
\operatorname{func2}(y : Y) \rightarrow A \\
\operatorname{func3}\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B
\end{Bmatrix}
\rightarrow
\begin{pmatrix}
a : A \\
b : B \\
c : C
\end{pmatrix}

### Stage Notation - Assigned:
Same as the default but with 'assigned to variable' notation on the left

s = \operatorname{Stage}\!\begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}
\rightarrow
\begin{pmatrix}
a : A \\
b : B \\
c : C
\end{pmatrix}

### Stage Notation - Assigned With Steps:
Same as the 'Stage Notation - With Steps' but with 'assigned to variable' notation on the left

s = \operatorname{Stage}\!\begin{pmatrix}
x : X \\
y : Y \\
z : Z
\end{pmatrix}
\begin{Bmatrix}
\operatorname{func1}(x : X) \rightarrow D \\
\operatorname{func2}(y : Y) \rightarrow A \\
\operatorname{func3}\begin{pmatrix} x : X \\ z : Z \end{pmatrix} \rightarrow B
\end{Bmatrix}
\rightarrow
\begin{pmatrix}
a : A \\
b : B \\
c : C
\end{pmatrix}

### Stage Notation Variants
The variants are as described in 'Notation Variants' section:
- omit_input_names
- omit_output_names
- omit_input_output_names
- omit_argument_names
- omit_argument_types
- omit_return_types
- compact
- canonic: compact

## Stage Transform Notation
A stage transform (workflow transform) is a transform which has one or several other transforms serving as its stages.

### Stage Transform Notation - Default
Stage transform (workflow transform) notation includes:
- Inputs vector: transform inputs as 'name: Type' pairs
- Stage 'assigned' notation for each stage
 - As defined in 'Stage Notation Notation - Assigned' section
- Outputs vector: transform inputs as 'name: Type' pairs

- In the following example, inputs vector is on tje second line, outputs is next-to-last line, transform stages are in between.

\begin{aligned}
& \begin{pmatrix} x : X \\ y : Y \\ z : Z \end{pmatrix} \\
& s1 = \operatorname{Stage}1 : \begin{pmatrix} x : X \\ y : Y \\ z : Z \end{pmatrix} \rightarrow \begin{pmatrix} a : A \\ b : B \\ c : C \end{pmatrix} \\
& s2 = \operatorname{Stage}2 : \begin{pmatrix} a : A \\ b : B \\ c : C \end{pmatrix} \rightarrow \begin{pmatrix} d : D \\ e : E \\ f : F \end{pmatrix} \\
& s3 = \operatorname{Stage}3 : \begin{pmatrix} d : D \\ e : E \\ f : F \end{pmatrix} \rightarrow \begin{pmatrix} u : U \\ v : V \\ w : W \end{pmatrix} \\
& \begin{pmatrix} u : U \\ v : V \\ w : W \end{pmatrix}
\end{aligned}

### Stage Transform Notation - Canonic
Canonic notation combines the default Stage Transform Notation with 'With Steps' notation for stages,
and features typed outputs without value assignments:
- Stages: 'Stage Notation - Assigned With Steps' with: 
  - with_steps
  - omit_argument_names
  - omit_argument_types
  - omit_return_types
  - omit_input_output_types
- Outputs: the outputs vector shows name : Type pairs without value assignments.

\begin{aligned}
& \begin{pmatrix} x : X \\ y : Y \\ z : Z \end{pmatrix} \\
& s1 = \operatorname{Stage}1\!\begin{pmatrix} x \\ y \\ z \end{pmatrix}
\begin{Bmatrix} \operatorname{func11} \\ \operatorname{func12} \\ \operatorname{func13} \end{Bmatrix}
\rightarrow \begin{pmatrix} a \\ b \\ c \end{pmatrix} \\
& s2 = \operatorname{Stage}2\!\begin{pmatrix} a \\ b \\ c \end{pmatrix} 
\begin{Bmatrix} \operatorname{func21} \\ \operatorname{func22} \\ \operatorname{func23} \end{Bmatrix}
\rightarrow \begin{pmatrix} d \\ e \\ f \end{pmatrix} \\
& s3 = \operatorname{Stage}3\!\begin{pmatrix} d \\ e \\ f \end{pmatrix} 
\begin{Bmatrix} \operatorname{func31} \\ \operatorname{func32} \\ \operatorname{func33} \end{Bmatrix}
\rightarrow \begin{pmatrix} u \\ v \\ w \end{pmatrix} \\
& \begin{pmatrix} u : U \\ v : V \\ w : W \end{pmatrix}
\end{aligned}

### Stage Transform Notation - Variants
The variants are as described in 'Notation Variants' section:
- omit_input_names
- omit_input_types
- omit_output_names
- omit_output_types
- omit_input_output_names
- omit_input_output_types
- compact: omit_input_output_names
- canonic 
