// Run from any directory: node docs/dev/auto/prose/checks/chunking.cjs
// Acceptance checks for the real Chunking family, not a prose generator or a visual renderer.
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const repo = path.resolve(__dirname, '../../../../..');
const read = p => fs.readFileSync(path.join(repo, p), 'utf8').replace(/\r\n/g, '\n');
const file = (phase, suffix) => `close/${phase}/search/transforms/chunking/Chunking.${suffix}.md`;
const section = (text, name) => {
    const parts = text.split(`\n## ${name}\n`);
    assert.equal(parts.length, 2, `exactly one ${name} section`);
    return parts[1].split(/\n## /)[0].trim();
};
const preamble = text => section(text, 'Implementation').split(/\n### /)[0].trim();
const blocks = (text, language) => [...text.matchAll(new RegExp(
    '(?:~~~|```)' + language + '\\n([\\s\\S]*?)\\n(?:~~~|```)', 'g'))].map(m => m[1]);
const formulae = text => [...text.matchAll(/^\$\$\n([\s\S]*?)\n\$\$$/gm)].map(m => m[1]);
const words = text => text.replace(/(?:~~~|```)\w*\n[\s\S]*?\n(?:~~~|```)/g, '')
    .replace(/^\$\$\n[\s\S]*?\n\$\$$/gm, '').replace(/\\\[[\s\S]*?\\\]/g, '')
    .trim().split(/\s+/).filter(Boolean).length;
const normalize = text => text.replace(/~~~text\n[\s\S]*?\n~~~/g, '<formula>')
    .replace(/^\$\$\n[\s\S]*?\n\$\$$/gm, '<formula>')
    .replace(/^- \*\*(.+?)\*\*\n  - (.+)$/gm, '- **$1**: $2')
    .replace(/^- \*\*(DocumentChunking|SentenceChunking)\*\*:/gm, '- $1:');
const compact = text => text.replace(/\s+/g, '');
const op = name => '\\operatorname{' + name.replaceAll('_', '\\_') + '}';
const methods = text => [...text.matchAll(/^    def (\w+)\(([\s\S]*?)\) -> ([^:\n]+):/gm)]
    .map(m => ({ name: m[1], args: m[2].replace(/^\s*self\s*,?\s*/, '').trim().replace(/,\s*$/, ''),
        returns: m[3], body: text.slice(m.index).split(/\n    (?:@|def )/)[0] }));
const sources = ['DocumentChunking', 'SentenceChunking'].map(name => ({ name,
    source: read(`examples/search/transforms/chunking/${name}.py`) }));
const inventory = sources.flatMap(s => methods(s.source));
const schemaSource = read('examples/search/schemas/chunking/intermediate.py');
const markedFields = [...schemaSource.split('class MarkedDocumentLine(Schema):')[1].split('\nclass ')[0]
    .matchAll(/^    (\w+) = /gm)].map(m => m[1]);
const referencePath = 'close/3/extended/search/transforms/chunking/Chunking.ext.md';
const reference = fs.existsSync(path.join(repo, referencePath)) ? read(referencePath) : null;
// Reference-specific shrinkage alarms, not general chapter word quotas. Editorial review is still required.
const minimum = { solution: reference ? Math.ceil(words(section(reference, 'Solution')) * 0.8) : 210,
    preamble: reference ? Math.ceil(words(preamble(reference)) * 0.8) : 180 };
function validate(draft, ext, form, collected) {
    for (const [name, text] of Object.entries({ draft, ext, form })) {
        assert.equal((text.match(/^# /gm) || []).length, 1, name + ': one H1');
        assert.ok((text.match(/search engine/gi) || []).length <= 1, name + ': phrase limit');
        assert.ok(words(section(text, 'Solution')) >= minimum.solution, name + ': Solution depth regression');
        assert.ok(words(preamble(text)) >= minimum.preamble, name + ': preamble missing or compressed');
        assert.ok(preamble(text).split(/\n\s*\n/).length >= 3, name + ': developed component narrative');
    }
    assert.deepEqual([...draft.matchAll(/^## (.+)$/gm)].map(m => m[1]),
        ['Problem', 'Solution', 'Builds on', 'Used by', 'Definitions', 'Inputs', 'Outputs', 'Stages',
            'Notation', 'Design', 'Implementation', 'Code'], 'Draft section contract');
    assert.equal(section(draft, 'Code'), 'Chunking.code.md');
    assert.equal(preamble(draft), section(draft, 'Implementation'), 'Draft has no stage tree');
    const extBody = ext.split('\n## Code\n')[0];
    const formBody = form.split('\n## Code\n')[0];
    assert.ok(!/^## (Notation|Design)$/m.test(extBody + '\n' + formBody));
    assert.equal(ext.split('\n## Code\n')[1], form.split('\n## Code\n')[1], 'Code verbatim');
    assert.equal(normalize(formBody), normalize(extBody), 'paragraph and topology preservation');
    assert.equal(preamble(form), preamble(ext), 'exact preamble preservation');
    assert.equal(normalize(section(draft, 'Solution')), normalize(section(ext, 'Solution')));
    const texts = blocks(extBody, 'text');
    const maths = formulae(formBody);
    assert.equal(texts.length, 12, 'Solution + eight groups + two shapes + Result');
    assert.equal(maths.length, texts.length, 'each text block must become a formula');
    assert.ok(!/^(?:~~~|```)/m.test(formBody), 'Form must contain no notation fences');
    assert.equal((formBody.match(/^\$\$$/gm) || []).length, maths.length * 2);
    for (const math of maths) {
        assert.ok(!/\\(?:tiny|scriptsize|footnotesize|small|normalsize|large|Large|LARGE|huge|Huge|fontsize|scalebox|resizebox)\b/.test(math),
            'uniform formula font size; wrap instead of shrinking');
        assert.ok(!math.includes('`') && !/(?<!\\)_/.test(math), 'escaped identifiers in math');
        const environments = [], braces = [];
        for (const match of math.matchAll(/\\(begin|end)\{(\w+)\}/g)) {
            if (match[1] === 'begin') environments.push(match[2]);
            else assert.equal(environments.pop(), match[2], 'nested math environments');
        }
        assert.equal(environments.length, 0);
        for (const match of math.matchAll(/(?<!\\)[{}]/g)) {
            if (match[0] === '{') braces.push('{'); else assert.equal(braces.pop(), '{');
        }
        assert.equal(braces.length, 0);
    }
    assert.equal(inventory.length, 12, 'source public method inventory');
    for (const method of inventory) {
        const signature = `${method.name}(${method.args}) -> ${method.returns}`;
        assert.ok(compact(section(ext, 'Implementation')).includes(compact(signature)), method.name + ': Extend signature');
        assert.ok(compact(section(draft, 'Notation')).includes(compact(signature)), method.name + ': Draft signature');
        const index = texts.findIndex(t => t.startsWith(method.name + '(') || t.includes('\n' + method.name + '('));
        assert.ok(index >= 0);
        const math = compact(maths[index]);
        const types = method.args.split(',').map(arg => arg.split(':')[1].trim());
        const args = types.length === 1 ? '(' + types[0] + ')' :
            '\\!\\begin{pmatrix}' + types.join('\\\\') + '\\end{pmatrix}';
        const returns = method.returns.startsWith('list[') ?
            '\\operatorname{list}\\{\\operatorname{dict}\\{str,object\\}\\}' : method.returns;
        assert.ok(math.includes(op(method.name) + args + '\\rightarrow' + returns), method.name + ': formula signature');
    }
    const full = maths[texts.findIndex(t => t.startsWith('mark_lines('))];
    const fullFields = full.split('\\begin{pmatrix}')[1].split('\\end{pmatrix}')[0]
        .split('\\\\').map(s => s.trim().replaceAll('\\_', '_'));
    assert.deepEqual(fullFields, markedFields, 'full returned schema from source');
    for (const method of inventory.filter(m => m.body.includes('.project('))) {
        // Explicit projected-return assignments begin one indentation level below return.
        const returned = method.body.slice(method.body.indexOf('return '));
        const fields = [...returned.matchAll(/^            (\w+)=/gm)].map(m => m[1]);
        if (method.name === 'number_paragraphs') fields.push('ordinal');
        if (!fields.length) continue;
        const index = texts.findIndex(t => t.startsWith(method.name + '(') || t.includes('\n' + method.name + '('));
        const part = maths[index].split(op(method.name))[1].split('\\rightarrow')[1]
            .split('\\begin{pmatrix}')[1].split('\\end{pmatrix}')[0];
        assert.deepEqual(part.split('\\\\').map(s => s.trim().replaceAll('\\_', '_')), ['\\vdots', ...fields],
            method.name + ': projected return fields from source');
    }
    for (const source of sources) {
        const index = texts.findIndex(t => t.startsWith(source.name + ':'));
        const names = methods(source.source).map(m => m.name);
        const shapeNames = [...maths[index].matchAll(/\\operatorname\{([^}]+)\}/g)]
            .slice(1).map(m => m[1].replaceAll('\\_', '_'));
        assert.deepEqual(shapeNames, names, source.name + ': complete method vector');
        const result = maths.at(-1);
        for (const name of names) assert.ok(result.includes(op(name)), 'internal Result call includes ' + name);
    }
    for (const [alias, name, bindings, outputs] of [
        ['documents_chunked', 'DocumentChunking', 'documents=documents', ['sections', 'paragraphs']],
        ['sentences_chunked', 'SentenceChunking', 'documents=documents, paragraphs=documents_chunked.paragraphs', ['sentences']],
    ]) {
        assert.ok(texts.at(-1).includes(`${alias} = ${name}(${bindings}) -> ${outputs.join(', ')}`));
        const call = maths.at(-1).split(op(name))[1];
        const inputPart = call.split('\\begin{pmatrix}')[1].split('\\end{pmatrix}')[0];
        assert.deepEqual(inputPart.split('\\\\').map(s => s.trim()), bindings.split(', ').map(b => b.split('=')[0]),
            'stage inputs are unqualified names only');
        const resultPart = call.split('\\rightarrow')[1]
            .split('\\begin{pmatrix}')[1].split('\\end{pmatrix}')[0];
        assert.deepEqual(resultPart.split('\\\\').map(s => s.trim()), outputs, 'unqualified stage output names');
    }
    assert.ok(!maths.at(-1).includes(op('Chunking')), 'Result omits parent transform name');
    assert.ok(!maths.at(-1).includes('='), 'Result contains no stage aliases or argument assignments');
    assert.deepEqual(blocks(section(ext, 'Code'), 'python'), blocks(collected, 'python'), 'exact collected listings');
    const strippedCode = section(ext, 'Code').replace(/^\d+\. /gm, '').replace(/^### /gm, '## ');
    assert.equal(strippedCode, collected.replace(/^# Chunking\n/, '').trim(), 'collected prose and grouping');
    for (const text of [ext, form]) {
        assert.deepEqual([...section(text, 'Implementation').matchAll(/^([①-⑳]) /gm)].map(m => m[1]),
            [...'①②③④⑤⑥⑦⑧'], 'Implementation numbering');
        assert.deepEqual([...section(text, 'Code').matchAll(/^(\d+)\. \*/gm)].map(m => Number(m[1])),
            [1, 2, 3, 4, 5, 6, 7, 8], 'independent Code numbering');
    }
}
const candidate = [read(file('draft', 'draft')), read(file('extended', 'ext')),
    read(file('form', 'form')), read(file('collected', 'cnd'))];
validate(...candidate);
const mutations = [
    ['text notation left in Form', (d, e, f, c) => [d, e, f.replace(/^\$\$\n[\s\S]*?\n\$\$/m,
        '~~~text\ntext(D, [a, b)) = D[a:b]\n~~~'), c]],
    ['missing preamble in both outputs', (d, e, f, c) => [d,
        e.replace(preamble(e), ''), f.replace(preamble(f), ''), c]],
    ['compressed Solution in every phase', (d, e, f, c) => [d, e, f].map(t =>
        t.replace(section(t, 'Solution'), 'Chunking exposes hierarchical spans.')).concat(c)],
    ['missing method formula', (d, e, f, c) => [d, e, f.replace(op('publish_paragraphs'), op('omitted')), c]],
    ['missing return field', (d, e, f, c) => [d, e, f.replace('line\\_ordinal \\\\\n', ''), c]],
    ['shrunk Result', (d, e, f, c) => [d, e, f.replace(formulae(f).at(-1), '\\small\n' + formulae(f).at(-1)), c]],
    ['named Result', (d, e, f, c) => [d, e, f.replace(formulae(f).at(-1), op('Chunking') + ':\n' + formulae(f).at(-1)), c]],
    ['assigned stage input', (d, e, f, c) => [d, e, f.replace(op('DocumentChunking') + '\\!\\begin{pmatrix} documents',
        op('DocumentChunking') + '\\!\\begin{pmatrix} documents=documents'), c]],
];
for (const [name, mutate] of mutations) assert.throws(() => validate(...mutate(...candidate)), name + ' must fail');
console.log(JSON.stringify({ status: 'PASS', family: 'Chunking', sourceMethods: inventory.length,
    displayFormulae: formulae(candidate[2]).length, mutationChecks: mutations.map(m => m[0]),
    narrativeWords: { reference: reference && { solution: words(section(reference, 'Solution')), preamble: words(preamble(reference)) },
        current: { solution: words(section(candidate[1], 'Solution')), preamble: words(preamble(candidate[1])) } },
    visualRendering: 'Not checked by this script' }, null, 2));
